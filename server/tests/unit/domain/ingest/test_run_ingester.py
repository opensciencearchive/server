"""Tests for RunIngester — failure decisions are the policy's call (#152).

The handler catches RuntimeFailure facts from the runner, asks the
FailurePolicy what to do, and executes the verb: AbortRun → hard-stop the run;
Retry → re-raise for the worker's budgeted redelivery; GiveUp → fail the
ingestion with a queryable reason.
"""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.ingest.event.events import IngesterBatchReady, NextBatchRequested
from osa.domain.ingest.handler.run_ingester import RunIngester
from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, IngestStatus
from osa.domain.shared.error import TransientError
from osa.domain.shared.event import EventId
from osa.domain.shared.failure import FailureKind, FailurePolicy, RuntimeFailure
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.port.ingester_runner import IngesterOutput

_T0 = datetime(2026, 1, 1, tzinfo=UTC)


def _make_event(ingest_run_id: str = "run-1", batch_index: int = 0) -> NextBatchRequested:
    return NextBatchRequested(
        id=EventId(uuid4()),
        ingest_run_id=IngestRunId(ingest_run_id),
        convention_id="test-conv",
        batch_size=100,
        batch_index=batch_index,
    )


def _make_run(status: IngestStatus = IngestStatus.RUNNING) -> IngestRun:
    return IngestRun(
        id=IngestRunId("run-1"),
        convention_id="test-conv",
        status=status,
        batch_size=100,
        started_at=_T0,
    )


def _make_handler(
    *,
    run: IngestRun | None = None,
    runner_failure: RuntimeFailure | None = None,
) -> RunIngester:
    ingest_repo = AsyncMock()
    ingest_repo.get.return_value = run if run is not None else _make_run()

    convention_service = AsyncMock()
    conv = AsyncMock()
    conv.id = "test-conv"
    conv.ingester = IngesterDefinition(image="ghcr.io/x/ing:v1", digest="sha256:abc")
    convention_service.get_convention.return_value = conv

    ingester_runner = AsyncMock()
    ingester_runner.has_capacity.return_value = True
    if runner_failure is not None:
        ingester_runner.run.side_effect = runner_failure
    else:
        ingester_runner.run.return_value = IngesterOutput(
            records=[], session=None, files_dir=Path("/tmp/files")
        )

    ingest_storage = AsyncMock()
    ingest_storage.read_session.return_value = None
    ingest_storage.batch_work_dir.return_value = Path("/tmp/work")
    ingest_storage.batch_files_dir.return_value = Path("/tmp/files")

    return RunIngester(
        ingest_repo=ingest_repo,
        ingest_service=AsyncMock(),
        convention_service=convention_service,
        ingester_runner=ingester_runner,
        outbox=AsyncMock(),
        ingest_storage=ingest_storage,
        failure_policy=FailurePolicy(),
    )


def _emitted(handler: RunIngester) -> list:
    return [call[0][0] for call in handler.outbox.append.call_args_list]


class TestAbortOnEnvironmentalFailure:
    @pytest.mark.asyncio
    async def test_image_pull_failure_aborts_run(self) -> None:
        failure = RuntimeFailure(FailureKind.IMAGE_PULL, "Image pull failed: 401 Unauthorized")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        kwargs = handler.ingest_service.abort_run.await_args.kwargs
        assert handler.ingest_service.abort_run.await_args.args[0] == "run-1"
        assert kwargs["reason"] == "Image pull failed: 401 Unauthorized"
        assert kwargs["kind"] is FailureKind.IMAGE_PULL
        # No batch produced, no continuation scheduled, no per-batch failure.
        assert _emitted(handler) == []
        handler.ingest_service.fail_ingestion.assert_not_called()

    @pytest.mark.asyncio
    async def test_rbac_failure_aborts_run(self) -> None:
        failure = RuntimeFailure(FailureKind.RBAC, "K8s RBAC permission denied")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        assert handler.ingest_service.abort_run.await_args.kwargs["kind"] is FailureKind.RBAC


class TestRetryableFailuresReachTheWorker:
    @pytest.mark.asyncio
    async def test_timeout_re_raises_for_budgeted_redelivery(self) -> None:
        failure = RuntimeFailure(FailureKind.TIMEOUT, "Ingester timed out")
        handler = _make_handler(runner_failure=failure)

        with pytest.raises(TransientError):
            await handler.handle(_make_event())

        handler.ingest_service.abort_run.assert_not_called()
        handler.ingest_service.fail_ingestion.assert_not_called()

    @pytest.mark.asyncio
    async def test_upstream_exit_re_raises_for_budgeted_redelivery(self) -> None:
        failure = RuntimeFailure(FailureKind.UPSTREAM, "Source exited with code 3", exit_code=3)
        handler = _make_handler(runner_failure=failure)

        with pytest.raises(TransientError):
            await handler.handle(_make_event())


class TestGiveUpFailsIngestionWithReason:
    @pytest.mark.asyncio
    async def test_unknown_failure_fails_ingestion(self) -> None:
        failure = RuntimeFailure(FailureKind.UNKNOWN, "Source failed: PodFailurePolicy")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        kwargs = handler.ingest_service.fail_ingestion.await_args.kwargs
        assert handler.ingest_service.fail_ingestion.await_args.args[0] == "run-1"
        assert kwargs["reason"] == "Source failed: PodFailurePolicy"
        assert kwargs["kind"] is FailureKind.UNKNOWN

    @pytest.mark.asyncio
    async def test_oom_fails_ingestion(self) -> None:
        """Ingester Jobs have no memory-bump loop — an OOM gives up with the facts."""
        failure = RuntimeFailure(FailureKind.OOM, "Source killed by OOM")
        handler = _make_handler(runner_failure=failure)

        await handler.handle(_make_event())

        kwargs = handler.ingest_service.fail_ingestion.await_args.kwargs
        assert kwargs["kind"] is FailureKind.OOM


class TestExhaustedRetriesFailIngestion:
    @pytest.mark.asyncio
    async def test_on_exhausted_records_reason(self) -> None:
        handler = _make_handler()

        await handler.on_exhausted(_make_event())

        kwargs = handler.ingest_service.fail_ingestion.await_args.kwargs
        assert "exhausted" in kwargs["reason"]


class TestBatchIndexComesFromTheEvent:
    """batch_index is carried in the event, never re-derived from run counters (#160)."""

    @pytest.mark.asyncio
    async def test_batch_index_drives_storage_from_event_not_run_counter(self) -> None:
        # Run counter is ahead of the event's index (e.g. a pipelined redo);
        # the handler must honour the event, not the mutable counter.
        run = _make_run()
        run.batches_ingested = 7
        handler = _make_handler(run=run)

        await handler.handle(_make_event(batch_index=3))

        assert handler.ingester_runner.run.await_args.kwargs["inputs"].batch_index == 3
        handler.ingest_storage.batch_work_dir.assert_called_once_with("run-1", 3)
        handler.ingest_storage.batch_files_dir.assert_called_once_with("run-1", 3)
        handler.ingest_storage.write_records.assert_awaited_once()
        assert handler.ingest_storage.write_records.await_args.args[1] == 3

    @pytest.mark.asyncio
    async def test_continuation_event_advances_batch_index_by_one(self) -> None:
        handler = _make_handler()
        # Make the batch produce a session + records so has_more is True.
        handler.ingester_runner.run.return_value = IngesterOutput(
            records=[{"source_id": "a", "metadata": {}}],
            session={"cursor": "next"},
            files_dir=Path("/tmp/files"),
        )

        await handler.handle(_make_event(batch_index=4))

        continuations = [e for e in _emitted(handler) if isinstance(e, NextBatchRequested)]
        assert len(continuations) == 1
        assert continuations[0].batch_index == 5

    @pytest.mark.asyncio
    async def test_backpressure_requeue_keeps_the_same_batch_index(self) -> None:
        handler = _make_handler()
        handler.ingester_runner.has_capacity.return_value = False

        await handler.handle(_make_event(batch_index=6))

        requeues = [e for e in _emitted(handler) if isinstance(e, NextBatchRequested)]
        assert len(requeues) == 1
        assert requeues[0].batch_index == 6


class TestTerminalRunGuard:
    """Redelivered NextBatchRequested for an aborted run must be dropped."""

    @pytest.mark.asyncio
    async def test_failed_run_skips_ingester(self) -> None:
        handler = _make_handler(run=_make_run(status=IngestStatus.FAILED))

        await handler.handle(_make_event())

        handler.ingester_runner.run.assert_not_called()
        assert _emitted(handler) == []

    @pytest.mark.asyncio
    async def test_healthy_run_still_ingests(self) -> None:
        handler = _make_handler()

        await handler.handle(_make_event())

        handler.ingester_runner.run.assert_awaited_once()
        assert any(isinstance(e, IngesterBatchReady) for e in _emitted(handler))
