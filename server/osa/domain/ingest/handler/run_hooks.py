"""RunHooks — runs hook containers on an ingester batch."""

from pathlib import Path
from uuid import NAMESPACE_URL, uuid4, uuid5

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.model.ingester_record import IngesterRecord
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.error import NotFoundError, TransientError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.hook import HookIdentity, HookName
from osa.domain.shared.model.srn import ConventionSlug
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_release import HookRelease
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.service.hook import HookService
from osa.domain.validation.service.hook_registry import HookRegistryService
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)

# Stable namespace for deterministic hook_run ids (#145): uuid5 over
# (ingest_run_id, batch_index, hook_name) yields the same id across worker
# retries and duplicate deliveries of a batch, so record_run (ON CONFLICT DO
# NOTHING) never accumulates duplicate provenance rows.
_HOOK_RUN_NS = uuid5(NAMESPACE_URL, "osa:hook_run")


def _hook_run_id(ingest_run_id: str, batch_index: int, hook_name: HookName) -> HookRunId:
    """Deterministic hook_run id for one hook in one batch — stable across retries."""
    return HookRunId(uuid5(_HOOK_RUN_NS, f"{ingest_run_id}:{batch_index}:{hook_name.root}"))


class RunHooks(EventHandler[IngesterBatchReady]):
    """Runs hook containers on an ingester batch and emits HookBatchCompleted."""

    __claim_timeout__ = 3600.0
    __max_retries__ = 100

    ingest_repo: IngestRunRepository
    ingest_service: IngestService
    convention_service: ConventionService
    hook_service: HookService
    hook_registry: HookRegistryService
    outbox: Outbox
    ingest_storage: IngestStoragePort

    async def handle(self, event: IngesterBatchReady) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_id)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_id}")

        convention = await self.convention_service.get_convention(
            ConventionSlug.parse(ingest_run.convention_id)
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

        # Resolve each hook's live release ONCE and snapshot it for this run
        # (R8) so a mid-run deploy can't split the batch across versions.
        hook_names = list(convention.hooks)
        releases = await self.hook_registry.resolve_live(hook_names)
        pairs: list[tuple[HookIdentity, HookRelease]] = []
        for name in hook_names:
            hook = await self.hook_registry.get_hook(name)
            release = releases.get(name)
            if hook is None or release is None:
                raise NotFoundError(f"Hook {name!r} has no live release")
            pairs.append((HookIdentity(name=hook.name, feature=hook.feature), release))

        # Build work_dirs for each hook via storage port
        work_dirs: dict[HookName, Path] = {}
        for name in hook_names:
            work_dirs[name] = self.ingest_storage.hook_work_dir(
                event.ingest_run_id, event.batch_index, name.root
            )

        # Run every hook. Failures are values (HookExecution.failed), not
        # exceptions — one hook failing never discards another's outcome. Each
        # execution carries its own wall-clock window + total status.
        executions = await self.hook_service.run_hooks_for_batch(
            hook_releases=pairs,
            inputs=inputs,
            work_dirs=work_dirs,
        )

        short_id = event.ingest_run_id[:8]
        for e in executions:
            label = e.status.value if e.status is not None else (e.failure or "errored")
            log.info(
                "[{short_id}] batch {batch_index} hook={hook_name}: {status} in {duration:.1f}s",
                short_id=short_id,
                batch_index=event.batch_index,
                hook_name=e.hook_name,
                status=label,
                duration=e.duration_s,
                ingest_run_id=event.ingest_run_id,
            )

        # Any TRANSIENT failure → re-drive the whole batch. The UOW rolls back
        # (nothing recorded this attempt), but each hook's filesystem checkpoint
        # makes the re-run cheap — completed hooks return instantly without a
        # container, and the deterministic id keeps the eventual insert dup-free.
        pending = [e for e in executions if not e.is_terminal]
        if pending:
            names = ", ".join(e.hook_name.root for e in pending)
            raise TransientError(
                f"batch {event.batch_index}: {len(pending)} hook(s) pending retry: {names}"
            )

        # All hooks terminal → record provenance from each hook's OWN execution
        # (its real window + status; a PERMANENT/OOM failure is a terminal ERROR
        # run, not a batch failure — hooks are independent) and write run.json so
        # InsertBatchFeatures can stamp feature.run_id. record_run is idempotent.
        for e in executions:
            run_id = _hook_run_id(event.ingest_run_id, event.batch_index, e.hook_name)
            status = (
                HookRunStatus.from_hook_status(e.status)
                if e.status is not None
                else HookRunStatus.ERROR
            )
            # Persist a failed hook's container logs as a tenant-scoped artifact and
            # record the locator on the provenance row, so an ERROR run is
            # diagnosable (#145/#147). Passed hooks carry no logs → log_ref=None.
            log_ref: str | None = None
            if e.log_text is not None:
                log_ref = await self.ingest_storage.write_hook_log(
                    work_dirs[e.hook_name], e.log_text
                )
            await self.hook_registry.record_run(
                HookRun(
                    id=run_id,
                    release_id=e.release_id,
                    status=status,
                    started_at=e.started_at,
                    finished_at=e.finished_at,
                    duration_s=e.duration_s,
                    oom_retries=e.oom_retries,
                    log_ref=log_ref,
                )
            )
            await self.ingest_storage.write_run_ref(
                work_dirs[e.hook_name], str(run_id), str(e.release_id)
            )

        # Emit HookBatchCompleted (committed by the UOW on return). PublishBatch
        # publishes records that passed every hook; a permanently-failed hook just
        # means its records aren't complete, not that the batch failed.
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
