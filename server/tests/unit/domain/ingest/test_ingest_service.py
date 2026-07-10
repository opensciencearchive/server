"""T022: Unit tests for IngestService.start_ingest."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.ingest.model.ingest_run import Applied, IngestStatus, RunClosed
from osa.domain.ingest.service.ingest import IngestService
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import Domain


class RecordingIngestInstrumentation:
    """Records IngestInstrumentation calls for assertion (no MagicMock indirection)."""

    def __init__(self) -> None:
        self.batches_completed: list[int] = []
        self.batches_failed: list = []
        self.runs_finished: list[tuple] = []

    def batch_completed(self, *, published_count) -> None:  # noqa: ANN001
        self.batches_completed.append(published_count)

    def batch_failed(self, *, kind) -> None:  # noqa: ANN001
        self.batches_failed.append(kind)

    def run_finished(self, *, status, kind) -> None:  # noqa: ANN001
        self.runs_finished.append((status, kind))


def _make_convention(*, has_ingester: bool = True):
    conv = MagicMock()
    conv.srn = "test-conv"
    conv.ingester = (
        IngesterDefinition(
            image="ghcr.io/example/ingester:v1",
            digest="sha256:abc123",
        )
        if has_ingester
        else None
    )
    return conv


def _make_service(
    *,
    convention=None,
    running_ingest=None,
    convention_not_found: bool = False,
) -> IngestService:
    ingest_repo = AsyncMock()
    ingest_repo.get_running_for_convention.return_value = running_ingest
    ingest_repo.save = AsyncMock()

    convention_service = AsyncMock()
    if convention_not_found:
        convention_service.get_convention.side_effect = NotFoundError("Convention not found")
    else:
        convention_service.get_convention.return_value = convention or _make_convention()

    outbox = AsyncMock()

    return IngestService(
        ingest_repo=ingest_repo,
        convention_service=convention_service,
        outbox=outbox,
        node_domain=Domain("localhost"),
        instrumentation=RecordingIngestInstrumentation(),
    )


class TestStartIngest:
    @pytest.mark.asyncio
    async def test_creates_pending_ingest(self) -> None:
        service = _make_service()
        run = await service.start_ingest(
            convention_id="test-conv",
        )
        assert run.status == IngestStatus.PENDING
        assert run.convention_id == "test-conv"
        assert run.batch_size == 1000

    @pytest.mark.asyncio
    async def test_saves_and_emits_events(self) -> None:
        service = _make_service()
        run = await service.start_ingest(
            convention_id="test-conv",
        )
        service.ingest_repo.save.assert_called_once()
        assert service.outbox.append.call_count == 2

        # First event: IngestRunStarted (observability)
        first_event = service.outbox.append.call_args_list[0][0][0]
        assert first_event.__class__.__name__ == "IngestRunStarted"
        assert first_event.ingest_run_id == run.id
        assert first_event.convention_id == run.convention_id

        # Second event: NextBatchRequested (triggers first batch)
        second_event = service.outbox.append.call_args_list[1][0][0]
        assert second_event.__class__.__name__ == "NextBatchRequested"
        assert second_event.ingest_run_id == run.id
        assert second_event.convention_id == run.convention_id
        # First batch always pulls index 0 (#160).
        assert second_event.batch_index == 0

    @pytest.mark.asyncio
    async def test_custom_batch_size(self) -> None:
        service = _make_service()
        run = await service.start_ingest(
            convention_id="test-conv",
            batch_size=500,
        )
        assert run.batch_size == 500

    @pytest.mark.asyncio
    async def test_rejects_convention_not_found(self) -> None:
        service = _make_service(convention_not_found=True)
        with pytest.raises(NotFoundError):
            await service.start_ingest(
                convention_id="nonexistent",
            )

    @pytest.mark.asyncio
    async def test_rejects_no_ingester_configured(self) -> None:
        service = _make_service(convention=_make_convention(has_ingester=False))
        with pytest.raises(NotFoundError, match="No ingester configured"):
            await service.start_ingest(
                convention_id="test-conv",
            )

    @pytest.mark.asyncio
    async def test_rejects_ingest_already_running(self) -> None:
        existing = MagicMock()
        service = _make_service(running_ingest=existing)
        with pytest.raises(ConflictError, match="already running"):
            await service.start_ingest(
                convention_id="test-conv",
            )


class TestAbortRun:
    """abort_run — the AbortRun verb's executor (#152): hard-stop the whole run."""

    @pytest.mark.asyncio
    async def test_aborts_via_atomic_repo_update(self) -> None:
        from datetime import UTC, datetime

        from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        service.ingest_repo.abort.return_value = Applied(
            run=IngestRun(
                id=IngestRunId("run-1"),
                convention_id="test-conv",
                status=IngestStatus.FAILED,
                failure_reason="Image pull failed: 401",
                failure_kind=FailureKind.IMAGE_PULL,
                started_at=datetime.now(UTC),
            )
        )

        await service.abort_run(
            IngestRunId("run-1"), reason="Image pull failed: 401", kind=FailureKind.IMAGE_PULL
        )

        kwargs = service.ingest_repo.abort.await_args.kwargs
        assert service.ingest_repo.abort.await_args.args[0] == "run-1"
        assert kwargs["reason"] == "Image pull failed: 401"
        assert kwargs["kind"] is FailureKind.IMAGE_PULL
        assert kwargs["completed_at"] is not None

    @pytest.mark.asyncio
    async def test_abort_is_idempotent_when_run_already_terminal(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        # Repo returns RunClosed when the run was already terminal — no-op, no error.
        service.ingest_repo.abort.return_value = RunClosed()

        await service.abort_run(IngestRunId("run-1"), reason="rbac denied", kind=FailureKind.RBAC)


class TestFailIngestionRecordsReason:
    """fail_ingestion must leave a queryable explanation on the run (#152)."""

    @pytest.mark.asyncio
    async def test_records_failure_reason_and_kind(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        run = MagicMock()
        run.check_completion.return_value = False
        service.ingest_repo.increment_batches_ingested.return_value = Applied(run=run)
        service.ingest_repo.increment_failed.return_value = Applied(run=run)

        await service.fail_ingestion(
            IngestRunId("run-1"), reason="Source exited with code 3", kind=FailureKind.UPSTREAM
        )

        kwargs = service.ingest_repo.record_failure.await_args.kwargs
        assert service.ingest_repo.record_failure.await_args.args[0] == "run-1"
        assert kwargs["reason"] == "Source exited with code 3"
        assert kwargs["kind"] is FailureKind.UPSTREAM
        # Existing accounting is preserved: no more batches + one failed batch.
        service.ingest_repo.increment_batches_ingested.assert_awaited_once_with(
            "run-1", set_ingestion_finished=True
        )
        service.ingest_repo.increment_failed.assert_awaited_once_with("run-1")


class TestFailBatchRecordsReason:
    """A permanently-failed batch records a queryable reason (#152)."""

    @pytest.mark.asyncio
    async def test_records_reason_via_record_failure(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        run = MagicMock()
        run.check_completion.return_value = False
        service.ingest_repo.increment_failed.return_value = Applied(run=run)

        await service.fail_batch(
            IngestRunId("run-1"), reason="batch 3: hook retries exhausted", kind=None
        )

        service.ingest_repo.increment_failed.assert_awaited_once_with("run-1")
        kwargs = service.ingest_repo.record_failure.await_args.kwargs
        assert kwargs["reason"] == "batch 3: hook retries exhausted"
        assert kwargs["kind"] is None

    @pytest.mark.asyncio
    async def test_record_failure_runs_after_completion_save(self) -> None:
        """If the batch completes the run, _check_completion saves the aggregate
        (stale failure_reason=None). record_failure must run AFTER that save, or
        the reason gets clobbered."""
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        run = MagicMock(id="run-1", published_count=0)
        run.check_completion.return_value = True  # run completes in this call
        service.ingest_repo.increment_failed.return_value = Applied(run=run)

        await service.fail_batch(IngestRunId("run-1"), reason="boom", kind=None)

        names = [c[0] for c in service.ingest_repo.mock_calls if c[0]]
        assert names.index("save") < names.index("record_failure")

    @pytest.mark.asyncio
    async def test_fail_ingestion_records_reason_after_completion_save(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        run = MagicMock(id="run-1", published_count=0)
        run.check_completion.return_value = True
        service.ingest_repo.increment_batches_ingested.return_value = Applied(run=run)
        service.ingest_repo.increment_failed.return_value = Applied(run=run)

        await service.fail_ingestion(
            IngestRunId("run-1"), reason="source gave up", kind=FailureKind.UNKNOWN
        )

        names = [c[0] for c in service.ingest_repo.mock_calls if c[0]]
        assert names.index("save") < names.index("record_failure")


class TestTerminalRunAccountingIsFrozen:
    """A terminal run is immutable: late/stale batch accounting is a no-op (#152).

    After abort_run marks a run FAILED, in-flight batch deliveries can still
    reach on_exhausted; the guarded repo increments report the run as terminal
    (RunClosed) and the service must not mutate it further.
    """

    @pytest.mark.asyncio
    async def test_fail_batch_is_noop_when_run_is_terminal(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        service.ingest_repo.increment_failed.return_value = RunClosed()  # guard: run terminal

        await service.fail_batch(IngestRunId("run-1"), reason="late batch", kind=None)

        service.ingest_repo.record_failure.assert_not_awaited()
        service.outbox.append.assert_not_awaited()  # no IngestCompleted re-fired

    @pytest.mark.asyncio
    async def test_complete_batch_is_noop_when_run_is_terminal(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        service.ingest_repo.increment_completed.return_value = RunClosed()

        await service.complete_batch(IngestRunId("run-1"), published_count=5)

        service.outbox.append.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_fail_ingestion_is_noop_when_run_is_terminal(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        service.ingest_repo.increment_batches_ingested.return_value = RunClosed()

        await service.fail_ingestion(IngestRunId("run-1"), reason="late pull", kind=None)

        service.ingest_repo.increment_failed.assert_not_awaited()
        service.ingest_repo.record_failure.assert_not_awaited()


class TestIngestMetricsEmission:
    """IngestService emits batch/run telemetry at the Applied arms and terminal
    transitions — exactly once per terminal run."""

    @pytest.mark.asyncio
    async def test_complete_batch_emits_batch_completed(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        run = MagicMock(id="run-1", published_count=5)
        run.check_completion.return_value = False
        service.ingest_repo.increment_completed.return_value = Applied(run=run)

        await service.complete_batch(IngestRunId("run-1"), published_count=5)

        assert service.instrumentation.batches_completed == [5]
        assert service.instrumentation.runs_finished == []

    @pytest.mark.asyncio
    async def test_complete_batch_noop_emits_nothing(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId

        service = _make_service()
        service.ingest_repo.increment_completed.return_value = RunClosed()

        await service.complete_batch(IngestRunId("run-1"), published_count=5)

        assert service.instrumentation.batches_completed == []

    @pytest.mark.asyncio
    async def test_completion_emits_run_finished_once(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId

        service = _make_service()
        run = IngestRun(
            id=IngestRunId("run-1"),
            convention_id="test-conv",
            status=IngestStatus.RUNNING,
            ingestion_finished=True,
            batches_ingested=1,
            batches_completed=0,
            published_count=3,
            started_at=datetime.now(UTC),
        )
        # increment_completed returns the run that now satisfies completion.
        run.batches_completed = 1
        service.ingest_repo.increment_completed.return_value = Applied(run=run)

        await service.complete_batch(IngestRunId("run-1"), published_count=3)

        assert service.instrumentation.runs_finished == [(IngestStatus.COMPLETED, None)]

    @pytest.mark.asyncio
    async def test_fail_batch_emits_batch_failed(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        run = MagicMock(id="run-1", published_count=0)
        run.check_completion.return_value = False
        service.ingest_repo.increment_failed.return_value = Applied(run=run)

        await service.fail_batch(IngestRunId("run-1"), reason="boom", kind=FailureKind.HOOK_EXIT)

        assert service.instrumentation.batches_failed == [FailureKind.HOOK_EXIT]

    @pytest.mark.asyncio
    async def test_fail_ingestion_emits_batch_failed(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        run = MagicMock(id="run-1", published_count=0)
        run.check_completion.return_value = False
        service.ingest_repo.increment_batches_ingested.return_value = Applied(run=run)
        service.ingest_repo.increment_failed.return_value = Applied(run=run)

        await service.fail_ingestion(
            IngestRunId("run-1"), reason="upstream 500", kind=FailureKind.UPSTREAM
        )

        assert service.instrumentation.batches_failed == [FailureKind.UPSTREAM]

    @pytest.mark.asyncio
    async def test_abort_emits_run_finished_failed(self) -> None:
        from datetime import datetime as _dt

        from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        service.ingest_repo.abort.return_value = Applied(
            run=IngestRun(
                id=IngestRunId("run-1"),
                convention_id="test-conv",
                status=IngestStatus.FAILED,
                started_at=_dt.now(UTC),
            )
        )

        await service.abort_run(IngestRunId("run-1"), reason="rbac denied", kind=FailureKind.RBAC)

        assert service.instrumentation.runs_finished == [(IngestStatus.FAILED, FailureKind.RBAC)]

    @pytest.mark.asyncio
    async def test_abort_noop_emits_no_run_finished(self) -> None:
        from osa.domain.ingest.model.ingest_run import IngestRunId
        from osa.domain.shared.failure import FailureKind

        service = _make_service()
        service.ingest_repo.abort.return_value = RunClosed()

        await service.abort_run(IngestRunId("run-1"), reason="x", kind=FailureKind.RBAC)

        assert service.instrumentation.runs_finished == []
