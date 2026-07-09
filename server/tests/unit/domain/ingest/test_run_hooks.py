"""Tests for RunHooks — errors-as-values batch execution + idempotent retry (#145).

run_hooks_for_batch returns one total HookExecution per hook (never raises). The
handler records each hook from its own execution, continues on error, re-drives
the batch on a transient failure, and uses a deterministic hook_run id.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.handler.run_hooks import RunHooks, _hook_run_id
from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, IngestStatus
from osa.domain.shared.error import TransientError
from osa.domain.shared.failure import FailureKind, FailurePolicy
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookName, OciConfig, OciLimits, TableFeatureSpec
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId
from osa.domain.validation.model.hook_result import HookExecution, HookStatus
from osa.domain.validation.model.hook_run import HookRunStatus

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


class RecordingHookInstrumentation:
    """Records HookInstrumentation calls for assertion (no MagicMock indirection)."""

    def __init__(self) -> None:
        self.runs_finished: list[tuple] = []
        self.failures_decided: list[tuple] = []

    def run_finished(self, *, hook, status, duration_s, oom_retries) -> None:  # noqa: ANN001
        self.runs_finished.append((hook, status, duration_s, oom_retries))

    def run_failure_decided(self, *, hook, kind, decision) -> None:  # noqa: ANN001
        self.failures_decided.append((hook, kind, decision))


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


def _make_event(ingest_run_id: str = "run-1", batch_index: int = 0) -> IngesterBatchReady:
    return IngesterBatchReady(
        id=EventId(uuid4()),
        ingest_run_id=IngestRunId(ingest_run_id),
        batch_index=batch_index,
        has_more=False,
    )


def _make_registry(names: tuple[str, ...]) -> AsyncMock:
    registry = AsyncMock()
    hooks = {HookName(n): _make_hook(n) for n in names}
    registry.resolve_live.return_value = {HookName(n): _make_release(n) for n in names}

    async def get_hook(name: HookName) -> Hook | None:
        return hooks.get(name)

    registry.get_hook.side_effect = get_hook
    return registry


def _make_handler(*, hook_names: tuple[str, ...] = ("pockets",), executions=None) -> RunHooks:
    ingest_repo = AsyncMock()
    ingest_repo.get.return_value = IngestRun(
        id=IngestRunId("run-1"),
        convention_id="test-conv",
        status=IngestStatus.RUNNING,
        batch_size=100,
        started_at=_T0,
    )

    convention_service = AsyncMock()
    conv = AsyncMock()
    conv.hooks = [HookName(n) for n in hook_names]
    convention_service.get_convention.return_value = conv

    ingest_storage = AsyncMock()
    ingest_storage.read_records.return_value = [{"source_id": "rec-1", "metadata": {}, "files": []}]
    ingest_storage.batch_files_dir.return_value = Path("/tmp/files")
    ingest_storage.hook_work_dir.return_value = Path("/tmp/work")

    hook_service = AsyncMock()
    hook_service.run_hooks_for_batch.return_value = executions if executions is not None else []

    return RunHooks(
        ingest_repo=ingest_repo,
        ingest_service=AsyncMock(),
        convention_service=convention_service,
        hook_service=hook_service,
        hook_registry=_make_registry(hook_names),
        outbox=AsyncMock(),
        ingest_storage=ingest_storage,
        failure_policy=FailurePolicy(),
        instrumentation=RecordingHookInstrumentation(),
    )


def _emitted(handler: RunHooks) -> list:
    return [call[0][0] for call in handler.outbox.append.call_args_list]


def _recorded_runs(handler: RunHooks) -> list:
    return [call[0][0] for call in handler.hook_registry.record_run.call_args_list]


class TestContinueOnError:
    @pytest.mark.asyncio
    async def test_failed_hook_does_not_discard_passing_sibling(self) -> None:
        execs = [
            _passed_exec("hook_a", offset=0),
            _failed_exec("hook_b", FailureKind.HOOK_EXIT, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        runs = {r.id: r for r in _recorded_runs(handler)}
        assert len(runs) == 2  # both hooks recorded — sibling not erased
        by_status = sorted(r.status for r in runs.values())
        assert by_status == sorted([HookRunStatus.PASSED, HookRunStatus.ERROR])
        # Each run carries its OWN window (not a shared batch span).
        recorded = _recorded_runs(handler)
        for run, ex in zip(
            sorted(recorded, key=lambda r: r.started_at),
            sorted(execs, key=lambda e: e.started_at),
        ):
            assert run.started_at == ex.started_at and run.finished_at == ex.finished_at
        # Batch completes; one hook's permanent failure is not a batch failure.
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()


class TestLogRefCapture:
    @pytest.mark.asyncio
    async def test_failed_hook_with_logs_records_log_ref(self) -> None:
        execs = [_failed_exec("pockets", FailureKind.HOOK_EXIT, log_text="traceback...")]
        handler = _make_handler(executions=execs)
        handler.ingest_storage.write_hook_log.return_value = "/data/.../output/hook.log"

        await handler.handle(_make_event())

        # The container logs were persisted and the locator stamped on the run.
        handler.ingest_storage.write_hook_log.assert_awaited_once()
        assert handler.ingest_storage.write_hook_log.await_args.args[1] == "traceback..."
        assert _recorded_runs(handler)[0].log_ref == "/data/.../output/hook.log"

    @pytest.mark.asyncio
    async def test_passed_hook_has_no_log_ref(self) -> None:
        handler = _make_handler(executions=[_passed_exec("pockets")])

        await handler.handle(_make_event())

        handler.ingest_storage.write_hook_log.assert_not_called()
        assert _recorded_runs(handler)[0].log_ref is None


class TestDeterministicRunId:
    @pytest.mark.asyncio
    async def test_same_id_across_redeliveries(self) -> None:
        event = _make_event()
        ids = []
        for _ in range(2):
            handler = _make_handler(executions=[_passed_exec("pockets")])
            await handler.handle(event)
            ids.append(_recorded_runs(handler)[0].id)

        expected = _hook_run_id(event.ingest_run_id, event.batch_index, HookName("pockets"))
        assert ids[0] == ids[1] == expected


class TestTransientRetry:
    @pytest.mark.asyncio
    async def test_transient_hook_raises_and_does_not_emit(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.TIMEOUT)])

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        # No completion emitted and nothing recorded — the UOW will roll back and
        # the worker re-drives the batch.
        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.hook_registry.record_run.assert_not_called()


class TestTerminalFailurePolicy:
    @pytest.mark.asyncio
    async def test_permanent_failure_completes_batch(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.HOOK_EXIT)])

        await handler.handle(_make_event())

        assert _recorded_runs(handler)[0].status == HookRunStatus.ERROR
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_oom_exhaustion_completes_with_retry_count(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.OOM)])

        await handler.handle(_make_event())

        run = _recorded_runs(handler)[0]
        assert run.status == HookRunStatus.ERROR and run.oom_retries == 3
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()


class TestAbortOnEnvironmentalFailure:
    """An unpullable image / RBAC / config failure dooms every batch — the
    handler must abort the whole run instead of grinding through (#152)."""

    @pytest.mark.asyncio
    async def test_image_pull_failure_aborts_run(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.IMAGE_PULL)])

        await handler.handle(_make_event())

        kwargs = handler.ingest_service.abort_run.await_args.kwargs
        assert handler.ingest_service.abort_run.await_args.args[0] == "run-1"
        assert kwargs["kind"] is FailureKind.IMAGE_PULL
        assert kwargs["reason"] == "boom"
        # The batch is neither completed nor re-driven.
        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_abort_still_records_provenance(self) -> None:
        """The failed hook's ERROR run is recorded before the abort — facts survive."""
        execs = [
            _passed_exec("hook_a"),
            _failed_exec("hook_b", FailureKind.IMAGE_PULL, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        statuses = sorted(r.status for r in _recorded_runs(handler))
        assert statuses == sorted([HookRunStatus.PASSED, HookRunStatus.ERROR])
        handler.ingest_service.abort_run.assert_awaited_once()


class TestTerminalRunGuard:
    """Once a run is aborted, redelivered batch events must be dropped —
    that is what actually stops the scheduled work (#152)."""

    @pytest.mark.asyncio
    async def test_batch_for_failed_run_is_skipped(self) -> None:
        handler = _make_handler(executions=[_passed_exec("pockets")])
        handler.ingest_repo.get.return_value = IngestRun(
            id=IngestRunId("run-1"),
            convention_id="test-conv",
            status=IngestStatus.FAILED,
            batch_size=100,
            started_at=_T0,
        )

        await handler.handle(_make_event())

        handler.hook_service.run_hooks_for_batch.assert_not_called()
        assert _emitted(handler) == []
        handler.hook_registry.record_run.assert_not_called()


class TestDecisionPrecedence:
    """The batch's fate is the most severe decision across its hooks (#152)."""

    @pytest.mark.asyncio
    async def test_abort_dominates_retry_in_a_mixed_batch(self) -> None:
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


class TestBatchExhaustionRecordsReason:
    """A batch whose transient retries run out must leave a queryable reason (#152)."""

    @pytest.mark.asyncio
    async def test_on_exhausted_records_reason_and_batch(self) -> None:
        handler = _make_handler(executions=[_passed_exec("pockets")])

        await handler.on_exhausted(_make_event(batch_index=3))

        call = handler.ingest_service.fail_batch.await_args
        assert call.args[0] == "run-1"
        assert "exhausted" in call.kwargs["reason"]
        assert "3" in call.kwargs["reason"]
        assert call.kwargs["kind"] is None


class TestHookMetricsEmission:
    """RunHooks emits one run_finished per recorded execution and one
    run_failure_decided per (execution, decision)."""

    @pytest.mark.asyncio
    async def test_run_finished_emitted_per_execution(self) -> None:
        execs = [
            _passed_exec("hook_a", offset=0),
            _failed_exec("hook_b", FailureKind.HOOK_EXIT, offset=10),
        ]
        handler = _make_handler(hook_names=("hook_a", "hook_b"), executions=execs)

        await handler.handle(_make_event())

        inst = handler.instrumentation
        by_hook = {h.root: (status, dur, oom) for h, status, dur, oom in inst.runs_finished}
        assert by_hook["hook_a"] == (HookRunStatus.PASSED, 5.0, 0)
        assert by_hook["hook_b"] == (HookRunStatus.ERROR, 2.0, 0)

    @pytest.mark.asyncio
    async def test_failure_decided_emitted_for_failed_hook(self) -> None:
        from osa.domain.shared.failure import DecisionKind

        # HOOK_EXIT → GiveUp; the batch still completes.
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.HOOK_EXIT)])

        await handler.handle(_make_event())

        inst = handler.instrumentation
        assert len(inst.failures_decided) == 1
        hook, kind, decision = inst.failures_decided[0]
        assert hook.root == "pockets"
        assert kind is FailureKind.HOOK_EXIT
        assert decision is DecisionKind.GIVE_UP

    @pytest.mark.asyncio
    async def test_passed_only_batch_decides_no_failures(self) -> None:
        handler = _make_handler(executions=[_passed_exec("pockets")])

        await handler.handle(_make_event())

        assert handler.instrumentation.failures_decided == []
        assert len(handler.instrumentation.runs_finished) == 1

    @pytest.mark.asyncio
    async def test_abort_records_run_finished_and_decision(self) -> None:
        from osa.domain.shared.failure import DecisionKind

        # IMAGE_PULL → AbortRun; provenance is still recorded (run_finished fires).
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.IMAGE_PULL)])

        await handler.handle(_make_event())

        inst = handler.instrumentation
        assert len(inst.runs_finished) == 1
        assert inst.runs_finished[0][1] == HookRunStatus.ERROR
        assert inst.failures_decided[0][2] is DecisionKind.ABORT_RUN

    @pytest.mark.asyncio
    async def test_oom_run_finished_carries_retry_count(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.OOM)])

        await handler.handle(_make_event())

        # _failed_exec sets oom_retries=3 for OOM.
        assert handler.instrumentation.runs_finished[0][3] == 3
