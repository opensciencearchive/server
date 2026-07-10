"""ProcessBatch — one ingest batch orchestrated end-to-end as stages (#160).

Replaces the RunIngester→RunHooks→PublishBatch→InsertBatchFeatures
choreography with sequential stages inside a single delivery. Each stage is
guarded so a redelivery skips the work already durably recorded (the delivery
is the restart *trigger*; durable domain state is the restart *position*). The
#152 runtime-failure decision semantics (ingester per-failure match, hooks
``most_severe`` verdict) are absorbed here verbatim from the old handlers.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import ClassVar, assert_never
from uuid import NAMESPACE_URL, uuid4, uuid5

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.ingest.event.events import (
    HookBatchCompleted,
    IngestBatchPublished,
    IngesterBatchReady,
    NextBatchRequested,
)
from osa.domain.ingest.model.ingest_run import Applied, IngestRun, IngestStatus, RunClosed
from osa.domain.ingest.model.ingester_record import IngesterRecord
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.service import RecordService
from osa.domain.shared.error import NotFoundError, PermanentError, TransientError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.failure import (
    AbortRun,
    FailurePolicy,
    GiveUp,
    PriorAttempts,
    Retry,
    RetryWithMoreMemory,
    RuntimeFailure,
    most_severe,
)
from osa.domain.shared.model.hook import FeatureName, HookIdentity, HookName
from osa.domain.shared.model.source import IngestSource
from osa.domain.shared.model.srn import ConventionSlug, RecordSRN
from osa.domain.shared.model.workflow import WorkflowName, WorkflowStage
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterRunner
from osa.domain.shared.port.instrumentation import WorkflowInstrumentation
from osa.domain.shared.port.unit_of_work import UnitOfWork
from osa.domain.validation.model.batch_outcome import OutcomeStatus
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_release import HookRelease
from osa.domain.validation.model.hook_result import HookExecution
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.port.instrumentation import HookInstrumentation
from osa.domain.validation.service.hook import HookService
from osa.domain.validation.service.hook_registry import HookRegistryService
from osa.application.workflow.stages import StageRunner
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)

BACKPRESSURE_DELAY = timedelta(seconds=60)

# Stable namespace for deterministic hook_run ids (#145): uuid5 over
# (ingest_run_id, batch_index, hook_name) yields the same id across worker
# retries and duplicate deliveries of a batch, so record_run (ON CONFLICT DO
# NOTHING) never accumulates duplicate provenance rows.
_HOOK_RUN_NS = uuid5(NAMESPACE_URL, "osa:hook_run")


class ProcessBatch(EventHandler[NextBatchRequested]):
    """Orchestrates one ingest batch end-to-end as sequential stages (#160).

    Replaces RunIngester→RunHooks→PublishBatch→InsertBatchFeatures choreography.
    The delivery is the restart trigger; durable domain state is the restart
    position — see each stage's crash-safety note. Emitting the next
    NextBatchRequested right after the ingest stage preserves batch pipelining
    (batch N+1 sources while batch N runs hooks) across the worker pool.
    """

    __claim_timeout__: ClassVar[float] = 7200.0  # worst-case SUM of ingester + hooks stages
    __max_retries__: ClassVar[int] = 100  # sized for the flakiest stage (hooks)

    ingest_repo: IngestRunRepository
    ingest_service: IngestService
    convention_service: ConventionService
    ingester_runner: IngesterRunner
    ingest_storage: IngestStoragePort
    hook_service: HookService
    hook_registry: HookRegistryService
    record_service: RecordService
    record_repo: RecordRepository
    feature_service: FeatureService
    feature_storage: FeatureStoragePort
    outbox: Outbox
    failure_policy: FailurePolicy
    hook_instrumentation: HookInstrumentation
    instrumentation: WorkflowInstrumentation
    uow: UnitOfWork

    @staticmethod
    def _hook_run_id(ingest_run_id: str, batch_index: int, hook_name: HookName) -> HookRunId:
        """Deterministic hook_run id for one hook in one batch — stable across retries.

        Duplicate of ``run_hooks._hook_run_id`` for now; the original dies in the
        switchover package (P6) and this becomes the single home.
        """
        return HookRunId(uuid5(_HOOK_RUN_NS, f"{ingest_run_id}:{batch_index}:{hook_name.root}"))

    async def handle(self, event: NextBatchRequested) -> None:
        run = await self.ingest_repo.get(event.ingest_run_id)
        if run is None:
            # Deterministic missing resource — dead-letter now instead of burning
            # a container-sized retry budget (#160 decision 10).
            missing = NotFoundError(f"Ingest run not found: {event.ingest_run_id}")
            raise PermanentError(str(missing)) from missing

        # A finished/aborted run pulls no more batches — drop redelivered events
        # instead of sourcing work nobody will process (#152).
        if run.status in (IngestStatus.COMPLETED, IngestStatus.FAILED):
            log.warn(
                "batch {batch_index} skipped — run is {status}",
                batch_index=event.batch_index,
                status=run.status.value,
                ingest_run_id=event.ingest_run_id,
            )
            return

        ingest_pending = run.batches_ingested <= event.batch_index

        # Backpressure: don't source if the cluster can't schedule more Jobs. Only
        # relevant while ingest is pending — a re-driven post-ingest batch has no
        # container to submit here (#152).
        if ingest_pending and not await self.ingester_runner.has_capacity():
            log.info(
                "[{short_id}] backpressure: deferring next pull +{delay}s",
                short_id=event.ingest_run_id[:8],
                delay=int(BACKPRESSURE_DELAY.total_seconds()),
                ingest_run_id=event.ingest_run_id,
            )
            await self.outbox.append(
                NextBatchRequested(
                    id=EventId(uuid4()),
                    ingest_run_id=event.ingest_run_id,
                    convention_id=event.convention_id,
                    batch_size=event.batch_size,
                    batch_index=event.batch_index,
                ),
                deliver_after=datetime.now(UTC) + BACKPRESSURE_DELAY,
            )
            return

        convention = await self._get_convention(run.convention_id)
        runner = StageRunner(WorkflowName.PROCESS_BATCH, self.instrumentation)

        # ── INGEST ────────────────────────────────────────────────────────────
        if ingest_pending:
            async with runner.run(WorkflowStage.INGEST):
                stop = await self._ingest(event, run, convention)
            if stop:
                return
        else:
            runner.skipped(WorkflowStage.INGEST)

        # ── HOOKS ─────────────────────────────────────────────────────────────
        hooks = list(convention.hooks)
        if not hooks or await self._hooks_recorded(event, hooks):
            runner.skipped(WorkflowStage.HOOKS)
        else:
            async with runner.run(WorkflowStage.HOOKS):
                stop = await self._run_hooks(event, convention)
            if stop:
                return

        # ── PUBLISH ───────────────────────────────────────────────────────────
        async with runner.run(WorkflowStage.PUBLISH):
            mapping = await self._publish(event, run, convention)

        # ── INSERT_FEATURES ───────────────────────────────────────────────────
        async with runner.run(WorkflowStage.INSERT_FEATURES):
            await self._insert_features(event, convention, mapping)

        # Complete LAST and deliberately WITHOUT a uow.commit: it rides the
        # scope-exit commit atomically with mark_delivered, so batches_completed
        # increments exactly once (a crash before mark_delivered rolls it back
        # with the delivery, and the redo re-derives the same mapping).
        await self.ingest_service.complete_batch(event.ingest_run_id, published_count=len(mapping))

    async def _get_convention(self, convention_id: str) -> Convention:
        """Resolve the convention, mapping a deterministic miss to PermanentError (#160)."""
        try:
            return await self.convention_service.get_convention(ConventionSlug.parse(convention_id))
        except NotFoundError as exc:
            raise PermanentError(str(exc)) from exc

    async def _ingest(
        self, event: NextBatchRequested, run: IngestRun, convention: Convention
    ) -> bool:
        """INGEST stage: source one batch. Returns True to stop the whole handle.

        Crash-safe via ``mark_batch_ingested`` (idempotent counter) + redelivery:
        nothing durable is written before the commit that releases the tx, and
        the container simply re-runs on redelivery.
        """
        if run.status == IngestStatus.PENDING:
            run.mark_running()
            await self.ingest_repo.save(run)

        if convention.ingester is None:
            missing = NotFoundError(f"No ingester for convention {run.convention_id}")
            raise PermanentError(str(missing)) from missing

        batch_index = event.batch_index
        session = await self.ingest_storage.read_session(event.ingest_run_id)

        effective_batch_limit = run.batch_size
        if run.limit is not None:
            ingested_so_far = run.batches_ingested * run.batch_size
            remaining = run.limit - ingested_so_far
            if remaining <= 0:
                log.warn(
                    "Ignoring redelivered NextBatchRequested — limit already met "
                    "(batches_ingested={batches_ingested}, limit={limit})",
                    batches_ingested=run.batches_ingested,
                    limit=run.limit,
                    ingest_run_id=event.ingest_run_id,
                )
                await self.ingest_repo.increment_batches_ingested(
                    event.ingest_run_id, set_ingestion_finished=True
                )
                return True
            effective_batch_limit = min(run.batch_size, remaining)

        inputs = IngesterInputs(
            convention_id=convention.id,
            ingest_run_id=event.ingest_run_id,
            batch_index=batch_index,
            config=convention.ingester.config,
            limit=effective_batch_limit,
            session=session,
        )
        work_dir = self.ingest_storage.batch_work_dir(event.ingest_run_id, batch_index)
        files_dir = self.ingest_storage.batch_files_dir(event.ingest_run_id, batch_index)

        # Release the DB transaction before parking on the container: nothing
        # uncommitted is lost, and the ingester re-runs on redelivery.
        await self.uow.commit()

        try:
            output = await self.ingester_runner.run(
                ingester=convention.ingester,
                inputs=inputs,
                files_dir=files_dir,
                work_dir=work_dir,
            )
        except RuntimeFailure as failure:
            # The runner reports facts; the policy decides; we execute the verb.
            # Ingester Jobs have no memory-bump lever, so PriorAttempts carries
            # no bumps and an OOM decision degrades to a give-up below.
            decision = self.failure_policy.decide(failure, PriorAttempts(memory_bumps=0))
            match decision:
                case AbortRun(reason=reason, kind=kind):
                    log.error(
                        "[{short_id}] ingester failed for the whole run ({kind}): {error}",
                        short_id=event.ingest_run_id[:8],
                        kind=kind.value,
                        error=reason,
                        container_logs=failure.container_logs or "",
                        ingest_run_id=event.ingest_run_id,
                    )
                    await self.ingest_service.abort_run(
                        event.ingest_run_id, reason=reason, kind=kind
                    )
                case Retry():
                    # Re-raise for the worker's budgeted redelivery with backoff;
                    # exhaustion lands in on_exhausted → fail_ingestion.
                    raise TransientError(failure.detail) from failure
                case GiveUp(reason=reason, kind=kind):
                    log.error(
                        "[{short_id}] ingester gave up ({kind}): {error}",
                        short_id=event.ingest_run_id[:8],
                        kind=kind.value,
                        error=reason,
                        container_logs=failure.container_logs or "",
                        ingest_run_id=event.ingest_run_id,
                    )
                    await self.ingest_service.fail_ingestion(
                        event.ingest_run_id, reason=reason, kind=kind
                    )
                case RetryWithMoreMemory():
                    # Ingester Jobs have no memory-bump lever (unlike hooks, which
                    # retry inside HookService), so an OOM can't be remediated —
                    # degrade to a give-up carrying the original OOM facts.
                    log.error(
                        "[{short_id}] ingester OOM, no memory-bump lever: {error}",
                        short_id=event.ingest_run_id[:8],
                        error=failure.detail,
                        container_logs=failure.container_logs or "",
                        ingest_run_id=event.ingest_run_id,
                    )
                    await self.ingest_service.fail_ingestion(
                        event.ingest_run_id, reason=failure.detail, kind=failure.kind
                    )
                case _:
                    assert_never(decision)
            return True

        await self.ingest_storage.write_records(event.ingest_run_id, batch_index, output.records)
        if output.session:
            await self.ingest_storage.write_session(event.ingest_run_id, output.session)

        has_more = output.session is not None and len(output.records) > 0
        if has_more and run.limit is not None:
            total_sourced = (run.batches_ingested + 1) * run.batch_size
            if total_sourced >= run.limit:
                has_more = False

        # Idempotent counter advance keyed on THIS batch index (redelivery-safe).
        match await self.ingest_repo.mark_batch_ingested(
            event.ingest_run_id, batch_index, ingestion_finished=not has_more
        ):
            case RunClosed():
                log.warn(
                    "batch {batch_index} counter update no-op — run already terminal",
                    batch_index=batch_index,
                    ingest_run_id=event.ingest_run_id,
                )
                return True
            case Applied():
                pass

        await self.outbox.append(
            IngesterBatchReady(
                id=EventId(uuid4()),
                ingest_run_id=event.ingest_run_id,
                batch_index=batch_index,
                has_more=has_more,
            )
        )
        log.info(
            "[{short_id}] batch {batch_index}: pulled {record_count} records (has_more={has_more})",
            short_id=event.ingest_run_id[:8],
            batch_index=batch_index,
            record_count=len(output.records),
            has_more=has_more,
            ingest_run_id=event.ingest_run_id,
        )
        if has_more:
            await self.outbox.append(
                NextBatchRequested(
                    id=EventId(uuid4()),
                    ingest_run_id=event.ingest_run_id,
                    convention_id=event.convention_id,
                    batch_size=run.batch_size,
                    batch_index=event.batch_index + 1,
                )
            )

        # Checkpoint A: counter + next-request commit atomically, so the next
        # batch is emitted exactly once and the ingest skip guard is armed with it.
        await self.uow.commit()
        return False

    async def _hooks_recorded(self, event: NextBatchRequested, hooks: list[HookName]) -> bool:
        """True iff every hook's deterministic run row exists (hooks concluded).

        Provenance rows are recorded all-or-nothing before checkpoint B, and a
        Retry verdict raises BEFORE any provenance is recorded — so presence of
        all rows ⟹ the hooks stage already concluded for this batch.
        """
        for name in hooks:
            run_id = self._hook_run_id(event.ingest_run_id, event.batch_index, name)
            if await self.hook_registry.get_run(run_id) is None:
                return False
        return True

    async def _run_hooks(self, event: NextBatchRequested, convention: Convention) -> bool:
        """HOOKS stage: run every hook on the batch. Returns True to stop the handle.

        Crash-safe: provenance is recorded all-or-nothing before checkpoint B and
        Retry raises before any is recorded, so a redelivery's guard re-runs the
        stage cleanly (each hook's filesystem checkpoint keeps the re-run cheap).
        """
        raw_records = await self.ingest_storage.read_records(event.ingest_run_id, event.batch_index)
        records = IngesterRecord.from_dicts(raw_records)
        if not records:
            log.warn(
                "ingest batch {batch_index}: no records to process",
                batch_index=event.batch_index,
                ingest_run_id=event.ingest_run_id,
            )

        files_base = self.ingest_storage.batch_files_dir(event.ingest_run_id, event.batch_index)
        files_dirs: dict[str, Path] = {}
        for record in records:
            if record.files:
                files_dirs[record.source_id] = files_base / record.source_id

        inputs = HookInputs(
            records=[
                HookRecord(id=r.source_id, metadata=r.metadata, size_hint_mb=r.total_file_mb)
                for r in records
            ],
            run_id=f"{event.ingest_run_id}_b{event.batch_index}",
            files_dirs=files_dirs,
        )

        # Resolve each hook's live release ONCE and snapshot it (R8) so a mid-run
        # deploy can't split the batch across versions.
        hook_names = list(convention.hooks)
        releases = await self.hook_registry.resolve_live(hook_names)
        pairs: list[tuple[HookIdentity, HookRelease]] = []
        for name in hook_names:
            hook = await self.hook_registry.get_hook(name)
            release = releases.get(name)
            if hook is None or release is None:
                missing = NotFoundError(f"Hook {name!r} has no live release")
                raise PermanentError(str(missing)) from missing
            pairs.append((HookIdentity(name=hook.name, feature=hook.feature), release))

        work_dirs: dict[HookName, Path] = {
            name: self.ingest_storage.hook_work_dir(
                event.ingest_run_id, event.batch_index, name.root
            )
            for name in hook_names
        }

        # Release the DB transaction before parking on the hook containers.
        await self.uow.commit()

        # Run every hook. Failures are values (HookExecution.failure), not
        # exceptions — one hook failing never discards another's outcome.
        executions = await self.hook_service.run_hooks_for_batch(
            hook_releases=pairs, inputs=inputs, work_dirs=work_dirs
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

        # Ask the policy what each failure means; the batch's fate is the most
        # severe decision across its hooks: abort > retry > terminal give-up (#152).
        decided = [
            (
                e,
                self.failure_policy.decide(
                    e.as_failure(), PriorAttempts(memory_bumps=e.oom_retries)
                ),
            )
            for e in executions
            if e.failure is not None
        ]
        for e, decision in decided:
            self.hook_instrumentation.run_failure_decided(
                hook=e.hook_name, kind=e.as_failure().kind, decision=decision.kind_label
            )

        verdict = most_severe(d for _, d in decided)
        match verdict:
            case AbortRun(reason=reason, kind=kind):
                # Environmental — recurs identically for every batch. Record the
                # facts, hard-stop the run, and do NOT publish or re-drive.
                await self._record_provenance(event, executions, work_dirs)
                await self.ingest_service.abort_run(event.ingest_run_id, reason=reason, kind=kind)
                return True
            case Retry():
                # Re-drive the whole batch. Provenance is NOT recorded, so the
                # guard sees "not done" and re-runs. The worker's budget bounds it
                # (on_exhausted → fail_batch).
                names = ", ".join(e.hook_name.root for e, d in decided if isinstance(d, Retry))
                raise TransientError(f"batch {event.batch_index}: hook(s) pending retry: {names}")
            case GiveUp() | RetryWithMoreMemory() | None:
                # Nothing failed, or every failure is terminal at batch level.
                # Record each hook's own ERROR/PASS provenance and complete.
                await self._record_provenance(event, executions, work_dirs)
                await self.outbox.append(
                    HookBatchCompleted(
                        id=EventId(uuid4()),
                        ingest_run_id=event.ingest_run_id,
                        batch_index=event.batch_index,
                    )
                )
            case _:
                assert_never(verdict)

        # Checkpoint B: hook_runs durable (feature-table FK needs them) and the
        # provenance-presence skip guard is armed together with them.
        await self.uow.commit()
        return False

    async def _record_provenance(
        self,
        event: NextBatchRequested,
        executions: list[HookExecution],
        work_dirs: dict[HookName, Path],
    ) -> None:
        """Record each hook's run row + run.json from its own execution (verbatim #145)."""
        for e in executions:
            run_id = self._hook_run_id(event.ingest_run_id, event.batch_index, e.hook_name)
            status = (
                HookRunStatus.from_hook_status(e.status)
                if e.status is not None
                else HookRunStatus.ERROR
            )
            # Persist a failed hook's container logs as a tenant-scoped artifact and
            # record the locator on the provenance row (#145/#147). Passed hooks
            # carry no logs → log_ref=None.
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
            self.hook_instrumentation.run_finished(
                hook=e.hook_name,
                status=status,
                duration_s=e.duration_s,
                oom_retries=e.oom_retries,
            )

    async def _publish(
        self, event: NextBatchRequested, run: IngestRun, convention: Convention
    ) -> dict[str, RecordSRN]:
        """PUBLISH stage: bulk-publish passing records. Returns the batch's SRN map.

        Harmless-to-redo: ``bulk_publish`` is ON CONFLICT DO NOTHING and the map
        is recomputed from the DB, so a redo (where every insert conflicts) still
        recovers the mapping and emits no duplicate audit event.
        """
        raw_records = await self.ingest_storage.read_records(event.ingest_run_id, event.batch_index)
        ingester_records = IngesterRecord.from_dicts(raw_records)
        batch_dir = str(self.ingest_storage.batch_dir(event.ingest_run_id, event.batch_index))

        hook_names = list(convention.hooks)
        expected_features = [FeatureName(h.root) for h in convention.hooks]

        passed_records = await self._get_passed_records(ingester_records, batch_dir, hook_names)

        drafts = [
            RecordDraft(
                source=IngestSource(
                    id=f"{run.convention_id}:{record.source_id}",
                    ingest_run_id=run.id,
                    upstream_source=record.source_id,
                    batch_index=event.batch_index,
                ),
                metadata=record.metadata,
                convention_id=ConventionSlug.parse(run.convention_id),
                expected_features=expected_features,
            )
            for record in passed_records
        ]

        new = await self.record_service.bulk_publish(drafts)

        # DB-authoritative upstream→SRN map for THIS batch (earlier-batch dupes
        # carry their own batch_index and are excluded).
        mapping = await self.record_repo.srns_for_ingest_batch(
            str(event.ingest_run_id), event.batch_index
        )

        # Emit the audit event only on the first success (new rows present); on a
        # redo every draft conflicts (new is empty) so no duplicate event fires.
        if len(new) > 0:
            await self.outbox.append(
                IngestBatchPublished(
                    id=EventId(uuid4()),
                    ingest_run_id=event.ingest_run_id,
                    convention_id=run.convention_id,
                    batch_index=event.batch_index,
                    published_srns=[str(srn) for srn in mapping.values()],
                    published_count=len(mapping),
                    expected_features=expected_features,
                    upstream_to_record_srn={up: str(srn) for up, srn in mapping.items()},
                )
            )

        log.info(
            "[{short_id}] batch {batch_index}: published {new}/{passed} records",
            short_id=event.ingest_run_id[:8],
            batch_index=event.batch_index,
            new=len(new),
            passed=len(passed_records),
            ingest_run_id=event.ingest_run_id,
        )

        # Checkpoint C: records durable before feature inserts (separate engine + FK).
        await self.uow.commit()
        return mapping

    async def _get_passed_records(
        self,
        ingester_records: list[IngesterRecord],
        batch_dir: str,
        hooks: list[HookName],
    ) -> list[IngesterRecord]:
        """Records that passed ALL hooks (via the storage port). No hooks ⇒ all pass."""
        if not hooks:
            return ingester_records

        passed_ids: set[str] | None = None
        for hook_name in hooks:
            outcomes = await self.feature_storage.read_batch_outcomes(batch_dir, hook_name.root)
            if not outcomes:
                return []
            hook_passed = {rid for rid, o in outcomes.items() if o.status == OutcomeStatus.PASSED}
            passed_ids = hook_passed if passed_ids is None else passed_ids & hook_passed

        if not passed_ids:
            return []
        return [r for r in ingester_records if r.source_id in passed_ids]

    async def _insert_features(
        self,
        event: NextBatchRequested,
        convention: Convention,
        mapping: dict[str, RecordSRN],
    ) -> None:
        """INSERT_FEATURES stage: stamp feature rows per published record.

        Harmless-to-redo via the feature store's replace-by-record semantics. The
        upstream→record map is the DB-recomputed ``mapping``, not an event payload.
        """
        expected_features = [FeatureName(h.root) for h in convention.hooks]
        if not expected_features or not mapping:
            return

        batch_output_dir = str(
            self.ingest_storage.batch_dir(event.ingest_run_id, event.batch_index)
        )
        total_inserted = 0
        skipped_dupes = 0

        for feature in expected_features:
            name = feature.root
            outcomes = await self.feature_storage.read_batch_outcomes(batch_output_dir, name)
            run_ref = await self.feature_storage.read_run_ref(batch_output_dir, name)
            if run_ref is None:
                log.warn(
                    "no run.json for feature {feature} in batch {batch_index}; "
                    "skipping feature insert (no provenance)",
                    feature=name,
                    batch_index=event.batch_index,
                    ingest_run_id=event.ingest_run_id,
                )
                continue

            for upstream_id, outcome in outcomes.items():
                if outcome.status != OutcomeStatus.PASSED or not outcome.features:
                    continue
                record_srn = mapping.get(upstream_id)
                if record_srn is None:
                    # Cross-batch duplicate — published in an earlier batch, whose
                    # features were inserted then. Skip.
                    skipped_dupes += 1
                    continue
                total_inserted += await self.feature_service.insert_features(
                    feature=feature,
                    record_srn=str(record_srn),
                    rows=outcome.features,
                    run_id=run_ref.run_id,
                )

        dupe_msg = f", {skipped_dupes} duplicates skipped" if skipped_dupes else ""
        log.info(
            "[{short_id}] batch {batch_index}: inserted {total_inserted} feature rows "
            "({hook_count} hooks{dupe_msg})",
            short_id=event.ingest_run_id[:8],
            batch_index=event.batch_index,
            total_inserted=total_inserted,
            hook_count=len(expected_features),
            dupe_msg=dupe_msg,
            ingest_run_id=event.ingest_run_id,
        )

    async def on_exhausted(self, event: NextBatchRequested) -> None:
        """Workflow retries exhausted — account for the failure per stage (#152).

        If the batch was already sourced, fail just the batch (the run may still
        complete from its other batches); otherwise the ingester never delivered,
        so fail the ingestion (which latches ingestion_finished so the run closes).
        """
        run = await self.ingest_repo.get(event.ingest_run_id)
        if run is None:
            log.error(
                "workflow exhausted for missing run",
                ingest_run_id=event.ingest_run_id,
                batch_index=event.batch_index,
            )
            return

        if run.batches_ingested > event.batch_index:
            await self.ingest_service.fail_batch(
                event.ingest_run_id,
                reason=f"batch {event.batch_index}: workflow retries exhausted",
                kind=None,
            )
        else:
            await self.ingest_service.fail_ingestion(
                event.ingest_run_id, reason="ingester retries exhausted", kind=None
            )
