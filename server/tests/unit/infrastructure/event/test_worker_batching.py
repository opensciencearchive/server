"""Unit tests for Worker batch processing.

Tests for batch_size configuration and batch accumulation behavior.
"""

from datetime import UTC, datetime
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.event import (
    ClaimResult,
    Event,
    EventHandler,
    EventId,
)
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for worker tests."""

    id: EventId
    data: str


def make_mock_container(
    outbox: AsyncMock,
    session: AsyncMock | None = None,
    handler: EventHandler | None = None,
):
    """Create a mock DI container."""
    if session is None:
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.rollback = AsyncMock()

    async def get_dependency(cls):
        if cls == Outbox:
            return outbox
        if cls == AsyncSession:
            return session
        if handler is not None and (cls is type(handler) or issubclass(cls, EventHandler)):
            return handler
        return session

    scope = AsyncMock()
    scope.get = AsyncMock(side_effect=get_dependency)

    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=scope)
    context.__aexit__ = AsyncMock(return_value=None)

    container = MagicMock()
    container.return_value = context

    return container


class TestWorkerBatchSizeOne:
    """Tests for batch_size=1 (immediate processing)."""

    @pytest.mark.asyncio
    async def test_batch_size_one_processes_immediately(self):
        """batch_size=1 should call handle() for single event."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        claim_result = ClaimResult(events=[event], claimed_at=datetime.now(UTC))

        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        class ImmediateHandler(EventHandler[DummyEvent]):
            __batch_size__: ClassVar[int] = 1

            processed_events: list[DummyEvent]

            async def handle(self, event: DummyEvent) -> None:
                self.processed_events.append(event)

        handler = ImmediateHandler(processed_events=[])
        container = make_mock_container(outbox, session, handler)

        worker = Worker(ImmediateHandler)
        worker.set_container(container)

        # Act
        await worker._poll_once()

        # Assert - handle() called (not handle_batch())
        assert len(handler.processed_events) == 1
        assert handler.processed_events[0] == event


class TestWorkerBatchAccumulation:
    """Tests for batch accumulation with batch_size > 1."""

    @pytest.mark.asyncio
    async def test_batch_calls_handle_batch(self):
        """batch_size > 1 should call handle_batch() with all claimed events."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        events = [DummyEvent(id=EventId(uuid4()), data=f"event{i}") for i in range(5)]
        claim_result = ClaimResult(events=events, claimed_at=datetime.now(UTC))

        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        class BatchHandler(EventHandler[DummyEvent]):
            __batch_size__: ClassVar[int] = 100

            processed_batches: list[list[DummyEvent]]

            async def handle_batch(self, events: list[DummyEvent]) -> None:
                self.processed_batches.append(list(events))

        handler = BatchHandler(processed_batches=[])
        container = make_mock_container(outbox, session, handler)

        worker = Worker(BatchHandler)
        worker.set_container(container)

        # Act
        await worker._poll_once()

        # Assert - handle_batch() called with all events
        assert len(handler.processed_batches) == 1
        assert handler.processed_batches[0] == events


class TestWorkerBatchingIntegration:
    """Integration tests for different batch configurations."""

    @pytest.mark.asyncio
    async def test_different_handlers_different_batch_sizes(self):
        """Different handlers can have different batch sizes."""
        from osa.infrastructure.event.worker import Worker

        # Arrange handlers with different batch sizes
        class SmallBatchHandler(EventHandler[DummyEvent]):
            __batch_size__: ClassVar[int] = 1

            async def handle(self, event: DummyEvent) -> None:
                pass

        class LargeBatchHandler(EventHandler[DummyEvent]):
            __batch_size__: ClassVar[int] = 100

            async def handle_batch(self, events: list[DummyEvent]) -> None:
                pass

        # Create workers
        small_worker = Worker(SmallBatchHandler)
        large_worker = Worker(LargeBatchHandler)

        # Assert config is read correctly from classvars
        assert small_worker.config.batch_size == 1
        assert large_worker.config.batch_size == 100
