"""RunHooks — runs hook containers on an ingester batch."""

from pathlib import Path
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.model.ingester_record import IngesterRecord
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.service.hook import HookService
from osa.infrastructure.logging import get_logger
from osa.infrastructure.storage.layout import StorageLayout

log = get_logger(__name__)


class RunHooks(EventHandler[IngesterBatchReady]):
    """Runs hook containers on an ingester batch and emits HookBatchCompleted."""

    __claim_timeout__ = 3600.0

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    hook_service: HookService
    outbox: Outbox
    layout: StorageLayout

    async def handle(self, event: IngesterBatchReady) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_srn)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_srn}")

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(ingest_run.convention_srn)
        )

        # Read records from batch ingester dir
        ingester_dir = self.layout.ingest_batch_ingester_dir(
            event.ingest_run_srn, event.batch_index
        )
        records = IngesterRecord.from_jsonl(ingester_dir / "records.jsonl")

        if not records:
            log.warn(
                "ingest batch {batch_index}: no records to process",
                batch_index=event.batch_index,
                ingest_run_srn=event.ingest_run_srn,
            )

        # Build files_dirs from ingester files
        files_base = ingester_dir / "files"
        files_dirs: dict[str, Path] = {}
        if files_base.exists():
            for record in records:
                record_files = files_base / record.source_id
                if record_files.exists():
                    files_dirs[record.source_id] = record_files

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
            run_id=f"{event.ingest_run_srn}_batch{event.batch_index}",
            files_dirs=files_dirs,
        )

        # Build work_dirs for each hook
        work_dirs: dict[str, Path] = {}
        for hook in convention.hooks:
            hook_dir = self.layout.ingest_batch_hook_dir(
                event.ingest_run_srn, event.batch_index, hook.name
            )
            hook_dir.mkdir(parents=True, exist_ok=True)
            work_dirs[hook.name] = hook_dir

        # Run all hooks via HookService
        results = await self.hook_service.run_hooks_for_batch(
            hooks=convention.hooks,
            inputs=inputs,
            work_dirs=work_dirs,
        )

        short_id = event.ingest_run_srn.rsplit(":", 1)[-1][:8]
        for result in results:
            log.info(
                "[{short_id}] batch {batch_index} hook={hook_name}: {status} in {duration:.1f}s",
                short_id=short_id,
                batch_index=event.batch_index,
                hook_name=result.hook_name,
                status=result.status.value,
                duration=result.duration_seconds,
                ingest_run_srn=event.ingest_run_srn,
            )

        # Emit HookBatchCompleted
        await self.outbox.append(
            HookBatchCompleted(
                id=EventId(uuid4()),
                ingest_run_srn=event.ingest_run_srn,
                batch_index=event.batch_index,
            )
        )
