"""Tests for ProcessBatch — one ingest batch orchestrated end-to-end (#160).

ProcessBatch replaces the RunIngester→RunHooks→PublishBatch→InsertBatchFeatures
choreography with sequential stages inside one delivery. These tests port the
#152 decision-semantics coverage from the old per-handler tests (they must not
drift) and add new coverage for stage skipping, checkpoint ordering, and the
DB-recomputed publish mapping.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.application.workflow.process_batch import ProcessBatch
from osa.domain.ingest.event.events import (
    HookBatchCompleted,
    IngesterBatchReady,
    IngestBatchPublished,
    NextBatchRequested,
)
from osa.domain.ingest.model.ingester_release import IngesterReleaseId
from osa.domain.ingest.model.ingest_run import (
    Applied,
    IngestRun,
    IngestRunId,
    IngestStatus,
    RunClosed,
)
from osa.domain.shared.error import NotFoundError, PermanentError, TransientError
from osa.domain.shared.event import EventId
from osa.domain.shared.failure import DecisionKind, FailureKind, FailurePolicy, RuntimeFailure
from osa.domain.shared.model.hook import HookName, OciConfig, OciLimits, TableFeatureSpec
from osa.domain.shared.model.source import IngesterDefinition, IngesterName
from osa.domain.shared.model.srn import ConventionSlug, Domain, LocalId, RecordSRN, RecordVersion
from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage
from osa.domain.shared.port.ingester_runner import IngesterOutput
from osa.domain.validation.model.batch_outcome import (
    BatchRecordOutcome,
    HookRecordId,
    OutcomeStatus,
)
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId
from osa.domain.validation.model.hook_result import HookExecution, HookStatus
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus

_T0 = datetime(2026, 1, 1, tzinfo=UTC)
_UNSET = object()


# ── Recording test doubles ───────────────────────────────────────────────────


class _RecordingHookInstrumentation:
    """Records HookInstrumentation calls for assertion (no MagicMock indirection)."""

    def __init__(self) -> None:
        self.runs_finished: list[tuple] = []
        self.failures_decided: list[tuple] = []

    def run_finished(self, *, hook, status, duration_s, oom_retries) -> None:  # noqa: ANN001
        self.runs_finished.append((hook, status, duration_s, oom_retries))

    def run_failure_decided(self, *, hook, kind, decision) -> None:  # noqa: ANN001
        self.failures_decided.append((hook, kind, decision))


class _RecordingWorkflowInstrumentation:
    """Records every ``stage_finished`` call for assertion."""

    def __init__(self) -> None:
        self.stages: list[tuple[WorkflowName, WorkflowStage, StageOutcome]] = []

    def stage_finished(self, *, workflow, stage, outcome) -> None:  # noqa: ANN001
        self.stages.append((workflow, stage, outcome))


class _RecordingUnitOfWork:
    """Records the interleaving of commits against a shared timeline."""

    def __init__(self, timeline: list[str]) -> None:
        self.timeline = timeline
        self.commits = 0

    async def commit(self) -> None:
        self.commits += 1
        self.timeline.append("commit")


def _logged(timeline: list[str], label: str, result):  # noqa: ANN001, ANN201
    """An async side_effect that records ``label`` on the timeline then returns ``result``."""

    async def _fn(*args, **kwargs):  # noqa: ANN002, ANN003, ANN202
        timeline.append(label)
        return result

    return _fn


# ── Builders ─────────────────────────────────────────────────────────────────


def _make_event(ingest_run_id: str = "run-1", batch_index: int = 0) -> NextBatchRequested:
    return NextBatchRequested(
        id=EventId(uuid4()),
        ingest_run_id=IngestRunId(ingest_run_id),
        convention_id="test-conv",
        batch_size=100,
        batch_index=batch_index,
    )


def _make_run(
    status: IngestStatus = IngestStatus.RUNNING,
    *,
    batches_ingested: int = 0,
    limit: int | None = None,
    records_sourced: int = 0,
) -> IngestRun:
    return IngestRun(
        id=IngestRunId("run-1"),
        convention_id="test-conv",
        release_id=IngesterReleaseId(uuid4()),
        status=status,
        batch_size=100,
        batches_ingested=batches_ingested,
        limit=limit,
        records_sourced=records_sourced,
        started_at=_T0,
    )


def _srn(local: str) -> RecordSRN:
    return RecordSRN(domain=Domain("localhost"), id=LocalId(local), version=RecordVersion(1))


def _make_release(name: str = "pockets") -> HookRelease:
    return HookRelease(
        id=HookReleaseId(uuid4()),
        hook_name=HookName(name),
        version=1,
        runtime=OciConfig(image="ghcr.io/test/x:v1", digest="sha256:abc", limits=OciLimits()),
        source_ref="git:abc",
        built_at=_T0,
    )


def _make_hook(name: str = "pockets") -> Hook:
    return Hook(
        name=HookName(name),
        feature=TableFeatureSpec(cardinality="one", columns=[]),
        live_release_id=HookReleaseId(uuid4()),
        created_at=_T0,
    )


def _make_hook_run() -> HookRun:
    return HookRun(
        id=HookRunId(uuid4()),
        release_id=HookReleaseId(uuid4()),
        status=HookRunStatus.PASSED,
        started_at=_T0,
        finished_at=_T0 + timedelta(seconds=1),
        duration_s=1.0,
        oom_retries=0,
    )


def _passed_exec(name: str, *, offset: int = 0) -> HookExecution:
    start = _T0 + timedelta(seconds=offset)
    return HookExecution(
        hook_name=HookName(name),
        release_id=HookReleaseId(uuid4()),
        status=HookStatus.PASSED,
        started_at=start,
        finished_at=start + timedelta(seconds=5),
        duration_s=5.0,
    )


def _failed_exec(
    name: str, kind: FailureKind, *, offset: int = 0, log_text: str | None = None
) -> HookExecution:
    start = _T0 + timedelta(seconds=offset)
    return HookExecution(
        hook_name=HookName(name),
        release_id=HookReleaseId(uuid4()),
        status=None,
        started_at=start,
        finished_at=start + timedelta(seconds=2),
        duration_s=2.0,
        oom_retries=3 if kind == FailureKind.OOM else 0,
        failure=kind,
        error_message="boom",
        log_text=log_text,
    )


def _make_registry(names: tuple[str, ...], *, get_run_result: HookRun | None) -> AsyncMock:
    registry = AsyncMock()
    hooks = {HookName(n): _make_hook(n) for n in names}
    registry.resolve_live.return_value = {HookName(n): _make_release(n) for n in names}

    async def get_hook(name: HookName) -> Hook | None:
        return hooks.get(name)

    registry.get_hook.side_effect = get_hook
    registry.get_run.return_value = get_run_result
    return registry


def _make_handler(
    *,
    run: IngestRun | None = None,
    hook_names: tuple[str, ...] = ("pockets",),
    executions=None,  # noqa: ANN001
    runner_failure: RuntimeFailure | None = None,
    has_more: bool = True,
    outcomes=None,  # noqa: ANN001
    mapping: dict[str, RecordSRN] | None = None,
    published_count: int = 1,
    hooks_done: bool = False,
    has_capacity: bool = True,
    records=None,  # noqa: ANN001
    session_before: dict | None = None,
    output_session=_UNSET,  # noqa: ANN001
    ingester_records=None,  # noqa: ANN001
    stored_records: dict[int, list] | None = None,
) -> ProcessBatch:
    run = run if run is not None else _make_run()
    if executions is None:
        executions = [_passed_exec(hook_names[0])] if hook_names else []
    if records is None:
        records = [{"source_id": "rec-1", "metadata": {}, "files": []}]
    if outcomes is None:
        outcomes = {
            HookRecordId("rec-1"): BatchRecordOutcome(
                record_id=HookRecordId("rec-1"),
                status=OutcomeStatus.PASSED,
                features=[{"v": 1}],
            )
        }
    if mapping is None:
        mapping = {"rec-1": _srn("r-1")}

    timeline: list[str] = []

    convention_service = AsyncMock()
    conv = AsyncMock()
    conv.hooks = [HookName(n) for n in hook_names]
    conv.ingester = IngesterDefinition(
        name=IngesterName("from_pdb"),
        image="ghcr.io/x/ing:v1",
        digest="sha256:abc",
        source_ref="git+https://example.com/r@deadbeef",
    )
    conv.id = ConventionSlug.parse("test-conv")
    convention_service.get_convention.return_value = conv

    ingester_runner = AsyncMock()
    ingester_runner.has_capacity.return_value = has_capacity
    if runner_failure is not None:
        ingester_runner.run.side_effect = runner_failure
    else:
        pulled = (
            ingester_records
            if ingester_records is not None
            else [{"source_id": "rec-1", "metadata": {}}]
        )
        output = IngesterOutput(
            records=pulled,
            session=(
                ({"cursor": "next"} if has_more else None)
                if output_session is _UNSET
                else output_session
            ),
            files_dir=Path("/tmp/files"),
        )
        ingester_runner.run.side_effect = _logged(timeline, "ingest_run", output)

    ingest_storage = AsyncMock()
    ingest_storage.read_session.return_value = session_before
    if stored_records is None:
        ingest_storage.read_records.return_value = records
    else:

        async def _read_records(_run_id, batch_index):  # noqa: ANN001, ANN202
            return stored_records.get(batch_index, records)

        ingest_storage.read_records.side_effect = _read_records
    ingest_storage.write_hook_log.return_value = "/data/log"
    # Sync locator methods return Paths (not coroutines) so path arithmetic works.
    ingest_storage.batch_work_dir = MagicMock(return_value=Path("/tmp/work"))
    ingest_storage.batch_files_dir = MagicMock(return_value=Path("/tmp/files"))
    ingest_storage.hook_work_dir = MagicMock(return_value=Path("/tmp/hook"))
    ingest_storage.batch_dir = MagicMock(return_value=Path("/tmp/batch"))

    hook_service = AsyncMock()
    hook_service.run_hooks_for_batch.side_effect = _logged(timeline, "hooks_run", executions)

    feature_storage = AsyncMock()
    feature_storage.read_batch_outcomes.return_value = outcomes
    from osa.domain.shared.model.provenance import RunRef

    feature_storage.read_run_ref.return_value = RunRef(run_id="hr-1", release_id="rel-1")

    feature_service = AsyncMock()
    feature_service.insert_features.side_effect = _logged(timeline, "insert_features", 1)

    record_service = AsyncMock()
    record_service.bulk_publish.side_effect = _logged(
        timeline, "publish", [object()] * published_count
    )
    record_service.srns_for_ingest_batch.return_value = mapping

    ingest_service = AsyncMock()
    ingest_service.get_ingestion.return_value = run
    ingest_service.ensure_running.return_value = Applied(run)
    ingest_service.mark_batch_ingested.return_value = Applied(run)
    ingest_service.close_sourcing.return_value = Applied(run)
    ingest_service.complete_batch.side_effect = _logged(timeline, "complete", None)

    handler = ProcessBatch(
        ingest_service=ingest_service,
        convention_service=convention_service,
        ingester_runner=ingester_runner,
        ingest_storage=ingest_storage,
        hook_service=hook_service,
        hook_registry=_make_registry(
            hook_names, get_run_result=_make_hook_run() if hooks_done else None
        ),
        record_service=record_service,
        feature_service=feature_service,
        feature_storage=feature_storage,
        outbox=AsyncMock(),
        failure_policy=FailurePolicy(),
        hook_instrumentation=_RecordingHookInstrumentation(),
        instrumentation=_RecordingWorkflowInstrumentation(),
        uow=_RecordingUnitOfWork(timeline),
    )
    return handler


def _emitted(handler: ProcessBatch) -> list:
    return [call.args[0] for call in handler.outbox.append.call_args_list]


def _recorded_runs(handler: ProcessBatch) -> list:
    return [call.args[0] for call in handler.hook_registry.record_run.call_args_list]


# ── Happy path ───────────────────────────────────────────────────────────────


class TestHappyPath:
    @pytest.mark.asyncio
    async def test_full_batch_runs_all_stages_in_order(self) -> None:
        handler = _make_handler()

        await handler.handle(_make_event())

        stages = [(s, o) for _, s, o in handler.instrumentation.stages]
        assert stages == [
            (WorkflowStage.INGEST, StageOutcome.RAN),
            (WorkflowStage.HOOKS, StageOutcome.RAN),
            (WorkflowStage.PUBLISH, StageOutcome.RAN),
            (WorkflowStage.INSERT_FEATURES, StageOutcome.RAN),
        ]

    @pytest.mark.asyncio
    async def test_events_appended_across_the_batch(self) -> None:
        handler = _make_handler()

        await handler.handle(_make_event(batch_index=2))

        emitted = _emitted(handler)
        assert any(isinstance(e, IngesterBatchReady) for e in emitted)
        assert any(isinstance(e, HookBatchCompleted) for e in emitted)
        assert any(isinstance(e, IngestBatchPublished) for e in emitted)
        continuations = [e for e in emitted if isinstance(e, NextBatchRequested)]
        assert len(continuations) == 1
        assert continuations[0].batch_index == 3

    @pytest.mark.asyncio
    async def test_complete_batch_called_last_with_mapping_length(self) -> None:
        handler = _make_handler(mapping={"rec-1": _srn("r-1"), "rec-2": _srn("r-2")})

        await handler.handle(_make_event())

        handler.ingest_service.complete_batch.assert_awaited_once()
        call = handler.ingest_service.complete_batch.await_args
        assert call.kwargs["published_count"] == 2
        # complete rides the scope-exit commit — nothing commits after it.
        assert handler.uow.timeline[-1] == "complete"

    @pytest.mark.asyncio
    async def test_published_event_carries_full_mapping(self) -> None:
        mapping = {"rec-1": _srn("r-1"), "rec-2": _srn("r-2")}
        handler = _make_handler(mapping=mapping)

        await handler.handle(_make_event())

        published = next(e for e in _emitted(handler) if isinstance(e, IngestBatchPublished))
        assert published.published_count == 2
        assert set(published.upstream_to_record_srn) == {"rec-1", "rec-2"}


class TestCheckpointOrdering:
    @pytest.mark.asyncio
    async def test_commits_bracket_each_container_stage(self) -> None:
        handler = _make_handler()

        await handler.handle(_make_event())

        assert handler.uow.timeline == [
            "commit",  # release tx before ingester container
            "ingest_run",
            "commit",  # checkpoint A: counter + next-request atomic
            "commit",  # release tx before hook containers
            "hooks_run",
            "commit",  # checkpoint B: provenance durable
            "publish",
            "commit",  # checkpoint C: records durable before features
            "insert_features",
            "complete",
        ]


# ── Ingest stage: #152 decision arms (ported from test_run_ingester) ──────────


class TestIngestAbortOnEnvironmentalFailure:
    @pytest.mark.asyncio
    async def test_image_pull_failure_aborts_run(self) -> None:
        failure = RuntimeFailure(FailureKind.IMAGE_PULL, "Image pull failed: 401")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        kwargs = handler.ingest_service.abort_run.await_args.kwargs
        assert handler.ingest_service.abort_run.await_args.args[0] == "run-1"
        assert kwargs["reason"] == "Image pull failed: 401"
        assert kwargs["kind"] is FailureKind.IMAGE_PULL
        handler.hook_service.run_hooks_for_batch.assert_not_called()
        handler.ingest_service.fail_ingestion.assert_not_called()

    @pytest.mark.asyncio
    async def test_rbac_failure_aborts_run(self) -> None:
        failure = RuntimeFailure(FailureKind.RBAC, "K8s RBAC denied")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        assert handler.ingest_service.abort_run.await_args.kwargs["kind"] is FailureKind.RBAC


class TestIngestRetryReachesTheWorker:
    @pytest.mark.asyncio
    async def test_timeout_re_raises_for_budgeted_redelivery(self) -> None:
        failure = RuntimeFailure(FailureKind.TIMEOUT, "timed out")
        handler = _make_handler(runner_failure=failure)

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        handler.ingest_service.abort_run.assert_not_called()
        handler.ingest_service.fail_ingestion.assert_not_called()

    @pytest.mark.asyncio
    async def test_upstream_exit_re_raises(self) -> None:
        failure = RuntimeFailure(FailureKind.UPSTREAM, "exit 3", exit_code=3)
        handler = _make_handler(runner_failure=failure)

        with pytest.raises(TransientError):
            await handler.handle(_make_event())


class TestIngestGiveUpFailsIngestion:
    @pytest.mark.asyncio
    async def test_unknown_failure_fails_ingestion(self) -> None:
        failure = RuntimeFailure(FailureKind.UNKNOWN, "PodFailurePolicy")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        kwargs = handler.ingest_service.fail_ingestion.await_args.kwargs
        assert kwargs["reason"] == "PodFailurePolicy"
        assert kwargs["kind"] is FailureKind.UNKNOWN

    @pytest.mark.asyncio
    async def test_oom_degrades_to_fail_ingestion(self) -> None:
        """Ingester Jobs have no memory-bump lever — RetryWithMoreMemory degrades to give-up."""
        failure = RuntimeFailure(FailureKind.OOM, "killed by OOM")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        assert handler.ingest_service.fail_ingestion.await_args.kwargs["kind"] is FailureKind.OOM


class TestIngestSkip:
    @pytest.mark.asyncio
    async def test_already_ingested_batch_skips_ingest(self) -> None:
        # Counter is ahead of this batch → ingest already done.
        handler = _make_handler(run=_make_run(batches_ingested=5))

        await handler.handle(_make_event(batch_index=0))

        handler.ingester_runner.run.assert_not_called()
        # No second NextBatchRequested — pipelining is not re-triggered.
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        assert (
            WorkflowName.PROCESS_BATCH,
            WorkflowStage.INGEST,
            StageOutcome.SKIPPED,
        ) in handler.instrumentation.stages

    @pytest.mark.asyncio
    async def test_skip_still_runs_hooks_and_publish(self) -> None:
        handler = _make_handler(run=_make_run(batches_ingested=5))

        await handler.handle(_make_event(batch_index=0))

        handler.hook_service.run_hooks_for_batch.assert_awaited_once()
        handler.record_service.bulk_publish.assert_awaited_once()


class TestIngestLimitEarlyExit:
    @pytest.mark.asyncio
    async def test_limit_met_finishes_ingestion_and_stops(self) -> None:
        # The records actually sourced already meet the limit.
        run = _make_run(batches_ingested=1, limit=100, records_sourced=100)
        handler = _make_handler(run=run)

        await handler.handle(_make_event(batch_index=1))

        handler.ingest_service.close_sourcing.assert_awaited_once_with("run-1")
        handler.ingester_runner.run.assert_not_called()
        handler.hook_service.run_hooks_for_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_underfilled_batches_do_not_end_a_limited_run_early(self) -> None:
        """Three batches of 60 against a 250 limit must keep sourcing (#227).

        The old estimate read batches_ingested * batch_size = 300 and closed
        sourcing at 180 records actually pulled — a silent shortfall. Batches
        underfill routinely: the SDK skips records over max_file_mb.
        """
        run = _make_run(batches_ingested=3, limit=250, records_sourced=180)
        handler = _make_handler(run=run)

        await handler.handle(_make_event(batch_index=3))

        handler.ingest_service.close_sourcing.assert_not_awaited()
        handler.ingester_runner.run.assert_called_once()

    @pytest.mark.asyncio
    async def test_remaining_limit_caps_the_batch_it_asks_for(self) -> None:
        run = _make_run(batches_ingested=3, limit=250, records_sourced=180)
        handler = _make_handler(run=run)

        await handler.handle(_make_event(batch_index=3))

        inputs = handler.ingester_runner.run.call_args.kwargs["inputs"]
        assert inputs.limit == 70

    @pytest.mark.asyncio
    async def test_batch_count_is_reported_with_the_records_it_sourced(self) -> None:
        run = _make_run(batches_ingested=3, limit=250, records_sourced=180)
        handler = _make_handler(
            run=run,
            ingester_records=[{"source_id": f"rec-{i}", "metadata": {}} for i in range(70)],
        )

        await handler.handle(_make_event(batch_index=3))

        kwargs = handler.ingest_service.mark_batch_ingested.await_args.kwargs
        assert kwargs["record_count"] == 70

    @pytest.mark.asyncio
    async def test_has_more_closes_when_the_pulled_records_meet_the_limit(self) -> None:
        run = _make_run(batches_ingested=3, limit=250, records_sourced=180)
        handler = _make_handler(
            run=run,
            ingester_records=[{"source_id": f"rec-{i}", "metadata": {}} for i in range(70)],
        )

        await handler.handle(_make_event(batch_index=3))

        ready = next(e for e in _emitted(handler) if isinstance(e, IngesterBatchReady))
        assert ready.has_more is False
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))

    @pytest.mark.asyncio
    async def test_unlimited_run_asks_for_a_full_batch(self) -> None:
        handler = _make_handler(run=_make_run(batches_ingested=9, records_sourced=137))

        await handler.handle(_make_event(batch_index=9))

        inputs = handler.ingester_runner.run.call_args.kwargs["inputs"]
        assert inputs.limit == 100


class TestNonProgressGuard:
    """An ingester that re-emits its window without advancing dooms the run (#227).

    Continuation comes from ``ctx.set_session``, so handing back the state you
    were given guarantees the next pull is identical. Before #227 nothing
    detected that: the reported run executed 921 batches over several hours,
    re-pulling the same 1,000 records and publishing 0 each time.
    """

    @staticmethod
    def _stalling(**overrides) -> ProcessBatch:  # noqa: ANN003
        """A batch 1 that hands back batch 0's session and batch 0's window."""
        session = {"offset": 1000, "total": 7600}
        kwargs = dict(
            session_before=dict(session),
            output_session=dict(session),
            ingester_records=[{"source_id": "rec-1", "metadata": {}}],
            stored_records={0: [{"source_id": "rec-1", "metadata": {}}]},
        )
        kwargs.update(overrides)
        return _make_handler(**kwargs)

    @pytest.mark.asyncio
    async def test_repeated_window_with_unchanged_session_aborts_the_run(self) -> None:
        handler = self._stalling()

        await handler.handle(_make_event(batch_index=1))

        kwargs = handler.ingest_service.abort_run.await_args.kwargs
        assert kwargs["kind"] == FailureKind.NO_PROGRESS

    @pytest.mark.asyncio
    async def test_abort_reason_names_the_cause_and_the_remedy(self) -> None:
        handler = self._stalling()

        await handler.handle(_make_event(batch_index=1))

        reason = handler.ingest_service.abort_run.await_args.kwargs["reason"]
        assert "ctx.set_session" in reason

    @pytest.mark.asyncio
    async def test_stalled_batch_enqueues_nothing(self) -> None:
        handler = self._stalling()

        await handler.handle(_make_event(batch_index=1))

        # The loop is the bug: a stalled batch must not request another.
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        assert not any(isinstance(e, IngesterBatchReady) for e in _emitted(handler))

    @pytest.mark.asyncio
    async def test_stalled_batch_is_not_counted_or_persisted(self) -> None:
        handler = self._stalling()

        await handler.handle(_make_event(batch_index=1))

        handler.ingest_service.mark_batch_ingested.assert_not_awaited()
        handler.ingest_storage.write_records.assert_not_awaited()
        handler.ingest_storage.write_session.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stalled_batch_runs_no_hooks_and_publishes_nothing(self) -> None:
        handler = self._stalling()

        await handler.handle(_make_event(batch_index=1))

        handler.hook_service.run_hooks_for_batch.assert_not_called()
        handler.record_service.bulk_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_advancing_session_continues(self) -> None:
        handler = self._stalling(
            session_before={"offset": 1000},
            output_session={"offset": 2000},
        )

        await handler.handle(_make_event(batch_index=1))

        handler.ingest_service.abort_run.assert_not_awaited()
        assert any(isinstance(e, NextBatchRequested) for e in _emitted(handler))

    @pytest.mark.asyncio
    async def test_unchanged_session_with_new_records_continues(self) -> None:
        # A draining queue keeps a constant session marker and still advances;
        # the window is what distinguishes it from a loop.
        handler = self._stalling(
            ingester_records=[{"source_id": "rec-2", "metadata": {}}],
        )

        await handler.handle(_make_event(batch_index=1))

        handler.ingest_service.abort_run.assert_not_awaited()
        assert any(isinstance(e, NextBatchRequested) for e in _emitted(handler))

    @pytest.mark.asyncio
    async def test_first_batch_never_stalls(self) -> None:
        handler = _make_handler(session_before=None, output_session={"offset": 1000})

        await handler.handle(_make_event(batch_index=0))

        handler.ingest_service.abort_run.assert_not_awaited()
        handler.ingest_storage.read_records.assert_any_await("run-1", 0)

    @pytest.mark.asyncio
    async def test_final_batch_without_session_completes(self) -> None:
        handler = _make_handler(session_before={"offset": 7000}, output_session=None)

        await handler.handle(_make_event(batch_index=7))

        handler.ingest_service.abort_run.assert_not_awaited()
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))

    @pytest.mark.asyncio
    async def test_empty_pull_with_unchanged_session_is_not_a_stall(self) -> None:
        # Nothing pulled ends the run through has_more; it is not a loop.
        handler = self._stalling(ingester_records=[], stored_records={0: []})

        await handler.handle(_make_event(batch_index=1))

        handler.ingest_service.abort_run.assert_not_awaited()


class TestSessionRoundTripsThroughStorage:
    """The session an ingester sets is exactly what the next batch is handed (#227).

    The hole: persisting only a *truthy* session dropped the falsy-but-set
    ``{}``, so the next batch was handed the previous session instead, the
    guard never saw an unchanged session, and the run looped forever. These
    drive consecutive batches through storage that actually remembers, rather
    than injecting ``session_before`` by hand.
    """

    @staticmethod
    def _threaded(handler: ProcessBatch) -> dict:
        """Replace the storage double with one that remembers what it was told."""
        sessions: list[dict | None] = [None]
        written: dict[int, list] = {}

        async def write_session(_run_id, session):  # noqa: ANN001, ANN202
            sessions[0] = session

        async def read_session(_run_id):  # noqa: ANN001, ANN202
            return sessions[0]

        async def write_records(_run_id, batch_index, records):  # noqa: ANN001, ANN202
            written[batch_index] = records

        async def read_records(_run_id, batch_index):  # noqa: ANN001, ANN202
            return written.get(batch_index, [])

        handler.ingest_storage.write_session.side_effect = write_session
        handler.ingest_storage.read_session.side_effect = read_session
        handler.ingest_storage.write_records.side_effect = write_records
        handler.ingest_storage.read_records.side_effect = read_records
        return {"sessions": sessions, "records": written}

    @pytest.mark.asyncio
    async def test_an_empty_session_is_still_persisted(self) -> None:
        handler = _make_handler(output_session={})
        state = self._threaded(handler)

        await handler.handle(_make_event(batch_index=0))

        assert state["sessions"][0] == {}

    @pytest.mark.asyncio
    async def test_an_empty_session_repeated_aborts_on_the_next_batch(self) -> None:
        # The ingester sets `{}` every batch and re-emits the same record: the
        # next pull is identical, so batch 1 must be the end of it.
        handler = _make_handler(output_session={})
        self._threaded(handler)

        await handler.handle(_make_event(batch_index=0))
        await handler.handle(_make_event(batch_index=1))

        kwargs = handler.ingest_service.abort_run.await_args.kwargs
        assert kwargs["kind"] == FailureKind.NO_PROGRESS

    @pytest.mark.asyncio
    async def test_an_advancing_session_survives_the_round_trip(self) -> None:
        handler = _make_handler(output_session={"offset": 100})
        state = self._threaded(handler)

        await handler.handle(_make_event(batch_index=0))

        assert state["sessions"][0] == {"offset": 100}
        handler.ingest_service.abort_run.assert_not_awaited()


class TestRunTerminalizedBeforeSourcing:
    """A run cancelled between the skip guard and the start transition sources nothing.

    ``ensure_running`` is a guarded update, so it reports ``RunClosed`` when a
    cancel landed in that window instead of resurrecting the run as RUNNING.
    """

    @pytest.mark.asyncio
    async def test_run_closed_stops_before_the_container(self) -> None:
        handler = _make_handler()
        handler.ingest_service.ensure_running.return_value = RunClosed()

        await handler.handle(_make_event())

        handler.ingester_runner.run.assert_not_called()
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        handler.ingest_service.mark_batch_ingested.assert_not_awaited()


class TestMarkBatchIngestedRunClosed:
    @pytest.mark.asyncio
    async def test_run_closed_stops_without_emitting(self) -> None:
        handler = _make_handler()
        handler.ingest_service.mark_batch_ingested.return_value = RunClosed()

        await handler.handle(_make_event())

        assert not any(isinstance(e, IngesterBatchReady) for e in _emitted(handler))
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        handler.hook_service.run_hooks_for_batch.assert_not_called()


# ── Hooks stage: #152 decision arms (ported from test_run_hooks) ──────────────


class TestHooksContinueOnError:
    @pytest.mark.asyncio
    async def test_failed_hook_does_not_discard_passing_sibling(self) -> None:
        execs = [
            _passed_exec("hook_a", offset=0),
            _failed_exec("hook_b", FailureKind.HOOK_EXIT, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        statuses = sorted(r.status for r in _recorded_runs(handler))
        assert statuses == sorted([HookRunStatus.PASSED, HookRunStatus.ERROR])
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()


class TestHooksLogRefCapture:
    @pytest.mark.asyncio
    async def test_failed_hook_with_logs_records_log_ref(self) -> None:
        execs = [_failed_exec("pockets", FailureKind.HOOK_EXIT, log_text="traceback...")]
        handler = _make_handler(executions=execs)
        handler.ingest_storage.write_hook_log.return_value = "/data/hook.log"

        await handler.handle(_make_event())

        handler.ingest_storage.write_hook_log.assert_awaited_once()
        assert handler.ingest_storage.write_hook_log.await_args.args[1] == "traceback..."
        assert _recorded_runs(handler)[0].log_ref == "/data/hook.log"

    @pytest.mark.asyncio
    async def test_passed_hook_has_no_log_ref(self) -> None:
        handler = _make_handler(executions=[_passed_exec("pockets")])

        await handler.handle(_make_event())

        handler.ingest_storage.write_hook_log.assert_not_called()
        assert _recorded_runs(handler)[0].log_ref is None


class TestHooksDeterministicRunId:
    @pytest.mark.asyncio
    async def test_same_id_across_redeliveries(self) -> None:
        event = _make_event()
        ids = []
        for _ in range(2):
            handler = _make_handler(executions=[_passed_exec("pockets")])
            await handler.handle(event)
            ids.append(_recorded_runs(handler)[0].id)

        expected = ProcessBatch._hook_run_id(
            event.ingest_run_id, event.batch_index, HookName("pockets")
        )
        assert ids[0] == ids[1] == expected


class TestHooksTransientRetry:
    @pytest.mark.asyncio
    async def test_transient_hook_raises_and_records_nothing(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.TIMEOUT)])

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.hook_registry.record_run.assert_not_called()
        # Provenance-guard armed only after provenance is recorded → publish never ran.
        handler.record_service.bulk_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_hooks_stage_reports_failed_outcome(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.TIMEOUT)])

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        assert (
            WorkflowName.PROCESS_BATCH,
            WorkflowStage.HOOKS,
            StageOutcome.FAILED,
        ) in handler.instrumentation.stages


class TestHooksAbortOnEnvironmentalFailure:
    @pytest.mark.asyncio
    async def test_image_pull_aborts_and_records_provenance(self) -> None:
        execs = [
            _passed_exec("hook_a"),
            _failed_exec("hook_b", FailureKind.IMAGE_PULL, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        handler.ingest_service.abort_run.assert_awaited_once()
        assert handler.ingest_service.abort_run.await_args.kwargs["kind"] is FailureKind.IMAGE_PULL
        # Provenance survives the abort; the batch is neither completed nor published.
        statuses = sorted(r.status for r in _recorded_runs(handler))
        assert statuses == sorted([HookRunStatus.PASSED, HookRunStatus.ERROR])
        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.record_service.bulk_publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_abort_dominates_retry_in_a_mixed_batch(self) -> None:
        """Most-severe wins: a transient-retry hook alongside an abort-worthy one
        yields an abort, not a re-drive (#152 decision precedence)."""
        # hook_a wants a transient retry; hook_b hit an unpullable image.
        execs = [
            _failed_exec("hook_a", FailureKind.TIMEOUT),
            _failed_exec("hook_b", FailureKind.IMAGE_PULL, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        # Abort wins: the run is hard-stopped, the batch is NOT re-driven or completed.
        handler.ingest_service.abort_run.assert_awaited_once()
        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))


class TestHooksTerminalFailurePolicy:
    """GiveUp-class hook failures record an ERROR run and COMPLETE the batch —
    the failed feature is dropped, its passing siblings and records survive."""

    @pytest.mark.asyncio
    async def test_oom_exhaustion_completes_batch_with_retry_count(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.OOM)])

        await handler.handle(_make_event())

        run = _recorded_runs(handler)[0]
        assert run.status == HookRunStatus.ERROR and run.oom_retries == 3
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()


class TestHooksMetricsEmission:
    @pytest.mark.asyncio
    async def test_run_finished_emitted_per_execution(self) -> None:
        execs = [
            _passed_exec("hook_a", offset=0),
            _failed_exec("hook_b", FailureKind.HOOK_EXIT, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        by_hook = {
            h.root: (status, dur, oom)
            for h, status, dur, oom in handler.hook_instrumentation.runs_finished
        }
        assert by_hook["hook_a"] == (HookRunStatus.PASSED, 5.0, 0)
        assert by_hook["hook_b"] == (HookRunStatus.ERROR, 2.0, 0)

    @pytest.mark.asyncio
    async def test_failure_decided_emitted_for_failed_hook(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.HOOK_EXIT)])

        await handler.handle(_make_event())

        decided = handler.hook_instrumentation.failures_decided
        assert len(decided) == 1
        hook, kind, decision = decided[0]
        assert hook.root == "pockets"
        assert kind is FailureKind.HOOK_EXIT
        assert decision is DecisionKind.GIVE_UP

    @pytest.mark.asyncio
    async def test_passed_only_batch_decides_no_failures(self) -> None:
        handler = _make_handler(executions=[_passed_exec("pockets")])

        await handler.handle(_make_event())

        assert handler.hook_instrumentation.failures_decided == []
        assert len(handler.hook_instrumentation.runs_finished) == 1


class TestHooksSkip:
    @pytest.mark.asyncio
    async def test_recorded_hooks_skip_the_hooks_stage(self) -> None:
        handler = _make_handler(hooks_done=True)

        await handler.handle(_make_event())

        handler.hook_service.run_hooks_for_batch.assert_not_called()
        assert (
            WorkflowName.PROCESS_BATCH,
            WorkflowStage.HOOKS,
            StageOutcome.SKIPPED,
        ) in handler.instrumentation.stages
        # Publish still runs off the recomputed mapping.
        handler.record_service.bulk_publish.assert_awaited_once()


class TestZeroHooksConvention:
    @pytest.mark.asyncio
    async def test_no_hooks_skips_hooks_and_publishes_all(self) -> None:
        handler = _make_handler(hook_names=(), executions=[])

        await handler.handle(_make_event())

        handler.hook_service.run_hooks_for_batch.assert_not_called()
        assert (
            WorkflowName.PROCESS_BATCH,
            WorkflowStage.HOOKS,
            StageOutcome.SKIPPED,
        ) in handler.instrumentation.stages
        handler.record_service.bulk_publish.assert_awaited_once()
        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))


# ── Backpressure ─────────────────────────────────────────────────────────────


class TestBackpressure:
    @pytest.mark.asyncio
    async def test_no_capacity_with_pending_ingest_requeues_same_index(self) -> None:
        handler = _make_handler(has_capacity=False)

        await handler.handle(_make_event(batch_index=6))

        requeues = [e for e in _emitted(handler) if isinstance(e, NextBatchRequested)]
        assert len(requeues) == 1
        assert requeues[0].batch_index == 6
        assert handler.outbox.append.await_args.kwargs["deliver_after"] is not None
        handler.ingester_runner.run.assert_not_called()
        handler.hook_service.run_hooks_for_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_capacity_but_ingest_done_proceeds_to_hooks(self) -> None:
        handler = _make_handler(has_capacity=False, run=_make_run(batches_ingested=5))

        await handler.handle(_make_event(batch_index=0))

        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        handler.hook_service.run_hooks_for_batch.assert_awaited_once()


# ── Run guards ───────────────────────────────────────────────────────────────


class TestRunGuards:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "status",
        [IngestStatus.FAILED, IngestStatus.COMPLETED, IngestStatus.CANCELLED],
    )
    async def test_terminal_run_returns_early(self, status: IngestStatus) -> None:
        # Cancelling is how an operator stops a runaway run (#227): the guard is
        # what makes it take effect, by dropping every queued batch request.
        handler = _make_handler(run=_make_run(status=status))

        await handler.handle(_make_event())

        handler.convention_service.get_convention.assert_not_called()
        handler.ingester_runner.run.assert_not_called()
        assert _emitted(handler) == []

    @pytest.mark.asyncio
    async def test_missing_run_raises_permanent_error(self) -> None:
        handler = _make_handler()
        handler.ingest_service.get_ingestion.side_effect = NotFoundError("Ingest run not found")

        with pytest.raises(PermanentError):
            await handler.handle(_make_event())


# ── on_exhausted ─────────────────────────────────────────────────────────────


class TestOnExhausted:
    @pytest.mark.asyncio
    async def test_ingested_batch_fails_the_batch(self) -> None:
        handler = _make_handler(run=_make_run(batches_ingested=5))

        await handler.on_exhausted(_make_event(batch_index=3))

        call = handler.ingest_service.fail_batch.await_args
        assert call.args[0] == "run-1"
        assert "exhausted" in call.kwargs["reason"]
        assert "3" in call.kwargs["reason"]
        assert call.kwargs["kind"] is None
        handler.ingest_service.fail_ingestion.assert_not_called()

    @pytest.mark.asyncio
    async def test_unsourced_batch_fails_the_ingestion(self) -> None:
        handler = _make_handler(run=_make_run(batches_ingested=0))

        await handler.on_exhausted(_make_event(batch_index=0))

        handler.ingest_service.fail_ingestion.assert_awaited_once()
        assert handler.ingest_service.fail_ingestion.await_args.kwargs["kind"] is None
        handler.ingest_service.fail_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_run_logs_and_returns(self) -> None:
        handler = _make_handler()
        handler.ingest_service.get_ingestion.side_effect = NotFoundError("Ingest run not found")

        await handler.on_exhausted(_make_event())

        handler.ingest_service.fail_batch.assert_not_called()
        handler.ingest_service.fail_ingestion.assert_not_called()

    @pytest.mark.asyncio
    async def test_boundary_equal_index_fails_ingestion(self) -> None:
        # batches_ingested == batch_index → the batch was never sourced (the
        # counter only exceeds the index once ingest completes), so the ingestion
        # (not just the batch) is failed. Pins the boundary of the `>` guard.
        handler = _make_handler(run=_make_run(batches_ingested=3))

        await handler.on_exhausted(_make_event(batch_index=3))

        handler.ingest_service.fail_ingestion.assert_awaited_once()
        handler.ingest_service.fail_batch.assert_not_called()


# ── Hardening tests (#160) ────────────────────────────────────────────────────


class TestFinalAndLimitedBatches:
    @pytest.mark.asyncio
    async def test_final_empty_batch_closes_run(self) -> None:
        # The ingester's last pull yields no records and no session: has_more is
        # False, the run's ingestion is finished, no continuation is emitted, and
        # complete_batch reports zero published. The downstream stages still run
        # over the (empty) passed set.
        handler = _make_handler(
            records=[], outcomes={}, executions=[], mapping={}, published_count=0
        )
        empty_output = IngesterOutput(records=[], session=None, files_dir=Path("/tmp/files"))

        async def _empty_run(*_args: object, **_kwargs: object) -> IngesterOutput:
            return empty_output

        handler.ingester_runner.run.side_effect = _empty_run

        await handler.handle(_make_event())

        # ingestion latched finished, no continuation
        assert handler.ingest_service.mark_batch_ingested.await_args.kwargs["ingestion_finished"]
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))

        # downstream stages still run; nothing passed → publish over [] and count 0
        stages = [(s, o) for _, s, o in handler.instrumentation.stages]
        assert (WorkflowStage.HOOKS, StageOutcome.RAN) in stages
        assert (WorkflowStage.PUBLISH, StageOutcome.RAN) in stages
        assert (WorkflowStage.INSERT_FEATURES, StageOutcome.RAN) in stages
        handler.record_service.bulk_publish.assert_awaited_once_with([])
        assert handler.ingest_service.complete_batch.await_args.kwargs["published_count"] == 0

    @pytest.mark.asyncio
    async def test_limit_reached_exactly_stops_chaining(self) -> None:
        # The ingester returns records + a session (would normally chain), but
        # the records in hand reach the run's limit exactly, so has_more is
        # forced False: ingestion is latched finished and no next batch requested.
        handler = _make_handler(run=_make_run(batches_ingested=0, limit=1))

        await handler.handle(_make_event(batch_index=0))

        assert handler.ingest_service.mark_batch_ingested.await_args.kwargs["ingestion_finished"]
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        # the batch itself was still sourced + processed
        handler.ingester_runner.run.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_limit_met_redelivery_early_exits(self) -> None:
        # A redelivered request for a batch whose limit is already met: sourcing
        # is closed and the handler exits before touching the ingester or any
        # downstream stage.
        run = _make_run(batches_ingested=1, limit=100, records_sourced=100)
        handler = _make_handler(run=run)

        await handler.handle(_make_event(batch_index=1))

        handler.ingest_service.close_sourcing.assert_awaited_once_with("run-1")
        handler.ingester_runner.run.assert_not_called()
        handler.hook_service.run_hooks_for_batch.assert_not_called()
        handler.record_service.bulk_publish.assert_not_called()
        handler.ingest_service.complete_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_ingester_without_session_ends_run(self) -> None:
        # The ingester returns records but no session (upstream exhausted): the
        # records still flow to storage, but has_more is False so ingestion is
        # latched finished and no continuation is requested.
        handler = _make_handler(has_more=False)

        await handler.handle(_make_event())

        assert handler.ingest_service.mark_batch_ingested.await_args.kwargs["ingestion_finished"]
        assert not any(isinstance(e, NextBatchRequested) for e in _emitted(handler))
        # the batch's records were still persisted
        handler.ingest_storage.write_records.assert_awaited_once()
        assert handler.ingest_storage.write_records.await_args.args[2] == [
            {"source_id": "rec-1", "metadata": {}}
        ]


class TestPublishRedoRecovery:
    @pytest.mark.asyncio
    async def test_publish_redo_recovers_mapping_from_db(self) -> None:
        # A redo after a crash between publish and feature insertion: ingest +
        # hooks are skipped (counter ahead, provenance present), every draft now
        # conflicts (bulk_publish returns []), yet the upstream→SRN mapping is
        # recovered from the DB. No duplicate IngestBatchPublished fires, but the
        # feature stage runs against the recovered mapping and completes the batch.
        handler = _make_handler(
            run=_make_run(batches_ingested=5),
            hooks_done=True,
            published_count=0,
            mapping={"rec-1": _srn("r-1")},
        )

        await handler.handle(_make_event(batch_index=0))

        assert not any(isinstance(e, IngestBatchPublished) for e in _emitted(handler))
        handler.feature_service.insert_features.assert_awaited_once()
        assert handler.feature_service.insert_features.await_args.kwargs["record_srn"] == str(
            _srn("r-1")
        )
        assert handler.ingest_service.complete_batch.await_args.kwargs["published_count"] == 1

    @pytest.mark.asyncio
    async def test_cross_batch_duplicate_skipped_in_feature_insert(self) -> None:
        # Two records passed hooks, but only one is in this batch's SRN mapping
        # (the other was published by an earlier batch). Feature insertion runs
        # only for the mapped record; the cross-batch duplicate is skipped.
        outcomes = {
            HookRecordId("rec-1"): BatchRecordOutcome(
                record_id=HookRecordId("rec-1"),
                status=OutcomeStatus.PASSED,
                features=[{"v": 1}],
            ),
            HookRecordId("rec-2"): BatchRecordOutcome(
                record_id=HookRecordId("rec-2"),
                status=OutcomeStatus.PASSED,
                features=[{"v": 2}],
            ),
        }
        handler = _make_handler(outcomes=outcomes, mapping={"rec-1": _srn("r-1")})

        await handler.handle(_make_event())

        handler.feature_service.insert_features.assert_awaited_once()
        assert handler.feature_service.insert_features.await_args.kwargs["record_srn"] == str(
            _srn("r-1")
        )


class TestPartialProvenance:
    @pytest.mark.asyncio
    async def test_partial_provenance_reruns_all_hooks(self) -> None:
        # One hook's provenance row exists, the other's does not: the guard is
        # all-or-nothing, so the whole HOOKS stage re-runs — run_hooks_for_batch
        # is called with BOTH hooks paired to their releases.
        handler = _make_handler(
            hook_names=("hook_a", "hook_b"),
            executions=[_passed_exec("hook_a"), _passed_exec("hook_b", offset=5)],
        )
        event = _make_event()
        run_id_a = ProcessBatch._hook_run_id(
            event.ingest_run_id, event.batch_index, HookName("hook_a")
        )

        async def _get_run(rid: HookRunId) -> HookRun | None:
            return _make_hook_run() if rid == run_id_a else None

        handler.hook_registry.get_run.side_effect = _get_run

        await handler.handle(event)

        handler.hook_service.run_hooks_for_batch.assert_awaited_once()
        pairs = handler.hook_service.run_hooks_for_batch.await_args.kwargs["hook_releases"]
        paired_names = {identity.name.root for identity, _release in pairs}
        assert paired_names == {"hook_a", "hook_b"}


class TestHookRetryStageOutcome:
    @pytest.mark.asyncio
    async def test_hook_retry_emits_failed_stage_outcome(self) -> None:
        # A transient (Retry) hook verdict records the HOOKS stage FAILED and
        # short-circuits: neither PUBLISH nor INSERT_FEATURES stage outcomes are
        # recorded.
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.TIMEOUT)])

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        stages = handler.instrumentation.stages
        assert (
            WorkflowName.PROCESS_BATCH,
            WorkflowStage.HOOKS,
            StageOutcome.FAILED,
        ) in stages
        assert not any(stage == WorkflowStage.PUBLISH for _, stage, _ in stages)
        assert not any(stage == WorkflowStage.INSERT_FEATURES for _, stage, _ in stages)
