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
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import HookName, OciConfig, OciLimits, TableFeatureSpec
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId
from osa.domain.validation.model.hook_result import FailureKind, HookExecution, HookStatus
from osa.domain.validation.model.hook_run import HookRunStatus

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


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


def _failed_exec(name: str, kind: FailureKind, *, offset: int = 0) -> HookExecution:
    start = _T0 + timedelta(seconds=offset)
    return HookExecution(
        hook_name=HookName(name),
        release_id=HookReleaseId(uuid4()),
        status=None,
        started_at=start,
        finished_at=start + timedelta(seconds=2),
        duration_s=2.0,
        oom_retries=3 if kind == FailureKind.OOM_EXHAUSTED else 0,
        failure=kind,
        error_message="boom",
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
            _failed_exec("hook_b", FailureKind.PERMANENT, offset=10),
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
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.TRANSIENT)])

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        # No completion emitted and nothing recorded — the UOW will roll back and
        # the worker re-drives the batch.
        assert not any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.hook_registry.record_run.assert_not_called()


class TestTerminalFailurePolicy:
    @pytest.mark.asyncio
    async def test_permanent_failure_completes_batch(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.PERMANENT)])

        await handler.handle(_make_event())

        assert _recorded_runs(handler)[0].status == HookRunStatus.ERROR
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()

    @pytest.mark.asyncio
    async def test_oom_exhaustion_completes_with_retry_count(self) -> None:
        handler = _make_handler(executions=[_failed_exec("pockets", FailureKind.OOM_EXHAUSTED)])

        await handler.handle(_make_event())

        run = _recorded_runs(handler)[0]
        assert run.status == HookRunStatus.ERROR and run.oom_retries == 3
        assert any(isinstance(e, HookBatchCompleted) for e in _emitted(handler))
        handler.ingest_service.fail_batch.assert_not_called()
