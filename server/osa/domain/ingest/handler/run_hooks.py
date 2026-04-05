"""RunHooks — runs hook containers on an ingester batch."""

from pathlib import Path
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.model.ingester_record import IngesterRecord
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.error import NotFoundError, PermanentError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.domain.validation.service.hook import HookService
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)


class RunHooks(EventHandler[IngesterBatchReady]):
    """Runs hook containers on an ingester batch and emits HookBatchCompleted."""

    __claim_timeout__ = 3600.0
    __max_retries__ = 100

    ingest_repo: IngestRunRepository
    ingest_service: IngestService
    convention_service: ConventionService
    hook_service: HookService
    hook_runner: HookRunner
    outbox: Outbox
    ingest_storage: IngestStoragePort

    async def handle(self, event: IngesterBatchReady) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_id)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_id}")

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(ingest_run.convention_srn)
        )

        # Read records via storage port (filesystem or S3)
        raw_records = await self.ingest_storage.read_records(event.ingest_run_id, event.batch_index)
        records = IngesterRecord.from_dicts(raw_records)

        if not records:
            log.warn(
                "ingest batch {batch_index}: no records to process",
                batch_index=event.batch_index,
                ingest_run_id=event.ingest_run_id,
            )

        # Build files_dirs from ingester files (Path locators for runner volume mounts)
        files_base = self.ingest_storage.batch_files_dir(event.ingest_run_id, event.batch_index)
        files_dirs: dict[str, Path] = {}
        for record in records:
            if record.files:
                files_dirs[record.source_id] = files_base / record.source_id

        # Convert to HookInputs with size hints and file dirs
        inputs = HookInputs(
            records=[
                HookRecord(
                    id=r.source_id,
                    metadata=r.metadata,
                    size_hint_mb=r.total_file_mb,
                )
                for r in records
            ],
            run_id=f"{event.ingest_run_id}_b{event.batch_index}",
            files_dirs=files_dirs,
        )

        # Build work_dirs for each hook via storage port
        work_dirs: dict[str, Path] = {}
        for hook in convention.hooks:
            work_dirs[hook.name] = self.ingest_storage.hook_work_dir(
                event.ingest_run_id, event.batch_index, hook.name
            )

        # Run all hooks via HookService
        try:
            results = await self.hook_service.run_hooks_for_batch(
                hooks=convention.hooks,
                inputs=inputs,
                work_dirs=work_dirs,
            )
        except PermanentError as e:
            container_logs = await self.hook_runner.capture_logs(inputs.run_id)
            log.error(
                "[{short_id}] batch {batch_index} permanently failed: {error}",
                short_id=event.ingest_run_id[:8],
                batch_index=event.batch_index,
                error=str(e),
                container_logs=container_logs,
                ingest_run_id=event.ingest_run_id,
            )
            await self._fail_batch(event)
            return

        short_id = event.ingest_run_id[:8]
        for result in results:
            log.info(
                "[{short_id}] batch {batch_index} hook={hook_name}: {status} in {duration:.1f}s",
                short_id=short_id,
                batch_index=event.batch_index,
                hook_name=result.hook_name,
                status=result.status.value,
                duration=result.duration_seconds,
                ingest_run_id=event.ingest_run_id,
            )

        # Emit HookBatchCompleted
        await self.outbox.append(
            HookBatchCompleted(
                id=EventId(uuid4()),
                ingest_run_id=event.ingest_run_id,
                batch_index=event.batch_index,
            )
        )

    async def on_exhausted(self, event: IngesterBatchReady) -> None:
        """Called when transient retries are exhausted — account for the failed batch."""
        log.error(
            "batch {batch_index} retries exhausted",
            batch_index=event.batch_index,
            ingest_run_id=event.ingest_run_id,
        )
        await self._fail_batch(event)

    async def _fail_batch(self, event: IngesterBatchReady) -> None:
        """Account for a permanently failed batch."""
        await self.ingest_service.fail_batch(event.ingest_run_id)
