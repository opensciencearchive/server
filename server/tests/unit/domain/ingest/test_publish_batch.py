"""Tests for PublishBatch.on_exhausted — ensures failed batches are accounted for."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.ingest.event.events import HookBatchCompleted
from osa.domain.ingest.handler.publish_batch import PublishBatch
from osa.domain.ingest.model.ingest_run import IngestRun, IngestRunId, IngestStatus
from osa.domain.shared.event import EventId


def _make_event(
    ingest_run_id: str = "run-1",
    batch_index: int = 0,
) -> HookBatchCompleted:
    return HookBatchCompleted(
        id=EventId(uuid4()),
        ingest_run_id=IngestRunId(ingest_run_id),
        batch_index=batch_index,
    )


def _make_ingest_run(
    *,
    ingest_run_id: str = "run-1",
    batches_ingested: int = 3,
    batches_completed: int = 1,
    batches_failed: int = 0,
    ingestion_finished: bool = True,
) -> IngestRun:
    return IngestRun(
        id=IngestRunId(ingest_run_id),
        convention_srn="urn:osa:localhost:conv:test@1.0.0",
        status=IngestStatus.RUNNING,
        batches_ingested=batches_ingested,
        batches_completed=batches_completed,
        batches_failed=batches_failed,
        ingestion_finished=ingestion_finished,
        batch_size=100,
        started_at=datetime.now(UTC),
    )


def _make_handler() -> PublishBatch:
    ingest_service = AsyncMock()
    ingest_service.fail_batch = AsyncMock()

    return PublishBatch(
        ingest_repo=AsyncMock(),
        convention_service=AsyncMock(),
        record_service=AsyncMock(),
        feature_storage=AsyncMock(),
        outbox=AsyncMock(),
        ingest_storage=AsyncMock(),
        ingest_service=ingest_service,
    )


class TestPublishBatchOnExhausted:
    @pytest.mark.asyncio
    async def test_on_exhausted_calls_fail_batch(self) -> None:
        """When retries are exhausted, the batch must be accounted for as failed."""
        handler = _make_handler()
        event = _make_event()

        await handler.on_exhausted(event)

        handler.ingest_service.fail_batch.assert_called_once_with(
            IngestRunId("run-1"),
        )

    @pytest.mark.asyncio
    async def test_on_exhausted_exists(self) -> None:
        """PublishBatch must override on_exhausted (not rely on base class no-op)."""
        assert hasattr(PublishBatch, "on_exhausted")
        # Verify it's not the base class default
        from osa.domain.shared.event import EventHandler

        assert PublishBatch.on_exhausted is not EventHandler.on_exhausted
