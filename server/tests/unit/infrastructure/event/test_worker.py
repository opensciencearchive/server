"""Unit tests for Worker poll loop lifecycle.

Tests for pull-based event processing with EventHandler pattern.
"""

import asyncio
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
    WorkerStatus,
)
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for worker tests."""

    id: EventId
    data: str


class DummyHandler(EventHandler[DummyEvent]):
    """Test handler that tracks handle calls."""

    __batch_size__: ClassVar[int] = 10
    __poll_interval__: ClassVar[float] = 0.1

    processed_events: list[DummyEvent]

    async def handle(self, event: DummyEvent) -> None:
        self.processed_events.append(event)


class FailingHandler(EventHandler[DummyEvent]):
    """Handler that always raises an error."""

    async def handle(self, event: DummyEvent) -> None:
        raise RuntimeError("Processing failed")


def make_mock_container(
    outbox: AsyncMock,
    session: AsyncMock | None = None,
    handler: EventHandler | None = None,
):
    """Create a mock DI container that provides scoped dependencies.

    Creates a container mock that returns Outbox, AsyncSession, and handler
    when called as an async context manager with scope parameter.
    """
    if session is None:
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.rollback = AsyncMock()

    async def get_dependency(cls):
        """Return the appropriate dependency based on the requested class."""
        if cls == Outbox:
            return outbox
        if cls == AsyncSession:
            return session
        if handler is not None and (cls is type(handler) or issubclass(cls, EventHandler)):
            return handler
        return session

    # Create scope that returns dependencies
    scope = AsyncMock()
    scope.get = AsyncMock(side_effect=get_dependency)

    # Create async context manager
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=scope)
    context.__aexit__ = AsyncMock(return_value=None)

    # Container callable returns the context manager
    container = MagicMock()
    container.return_value = context

    return container


class TestWorkerPollLoop:
    """Tests for Worker poll loop lifecycle."""

    @pytest.mark.asyncio
    async def test_worker_claims_and_processes_events(self):
        """Worker should claim events and call handler.handle()."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        event1 = DummyEvent(id=EventId(uuid4()), data="event1")
        claim_result = ClaimResult(events=[event1], claimed_at=datetime.now(UTC))

        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        handler = DummyHandler(processed_events=[])
        container = make_mock_container(outbox, session, handler)

        worker = Worker(DummyHandler)
        worker.set_container(container)

        # Act - Run one poll cycle
        await worker._poll_once()

        # Assert
        assert len(handler.processed_events) == 1
        assert handler.processed_events[0] == event1
        outbox.claim.assert_called_once()
        outbox.mark_delivered.assert_called_once_with(event1.id)

    @pytest.mark.asyncio
    async def test_worker_returns_false_when_no_events(self):
        """Worker._poll_once should return False when no events are available."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(events=[], claimed_at=datetime.now(UTC))

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        handler = DummyHandler(processed_events=[])
        container = make_mock_container(outbox, session, handler)

        worker = Worker(DummyHandler)
        worker.set_container(container)

        # Act
        had_events = await worker._poll_once()

        # Assert - Should return False when no events (sleep happens in _run())
        assert had_events is False
        assert worker.state.status == WorkerStatus.IDLE

    @pytest.mark.asyncio
    async def test_worker_updates_state_during_processing(self):
        """Worker should update state as it processes events."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        claim_result = ClaimResult(events=[event], claimed_at=datetime.now(UTC))

        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        state_during_process: WorkerStatus | None = None

        class StateTrackingHandler(EventHandler[DummyEvent]):
            async def handle(self, event: DummyEvent) -> None:
                nonlocal state_during_process
                state_during_process = worker.state.status

        handler = StateTrackingHandler()
        container = make_mock_container(outbox, session, handler)

        worker = Worker(StateTrackingHandler)
        worker.set_container(container)

        # Act
        await worker._poll_once()

        # Assert - State should have been PROCESSING during handle()
        assert state_during_process == WorkerStatus.PROCESSING


class TestWorkerStartStop:
    """Tests for Worker.start() and Worker.stop()."""

    @pytest.mark.asyncio
    async def test_start_creates_asyncio_task(self):
        """Worker.start() should create an asyncio task."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(events=[], claimed_at=datetime.now(UTC))

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        handler = DummyHandler(processed_events=[])
        container = make_mock_container(outbox, session, handler)

        worker = Worker(DummyHandler)
        worker.set_container(container)

        # Act
        task = worker.start()

        # Assert
        assert isinstance(task, asyncio.Task)
        assert not task.done()

        # Cleanup
        worker.stop()
        await asyncio.sleep(0.15)  # Give time for graceful shutdown
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_start_requires_container(self):
        """Worker.start() should raise if container not set."""
        from osa.infrastructure.event.worker import Worker

        worker = Worker(DummyHandler)

        # Act & Assert - Should raise without container
        with pytest.raises(RuntimeError, match="Container not set"):
            worker.start()

    @pytest.mark.asyncio
    async def test_stop_signals_graceful_shutdown(self):
        """Worker.stop() should signal graceful shutdown."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(events=[], claimed_at=datetime.now(UTC))

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        handler = DummyHandler(processed_events=[])
        container = make_mock_container(outbox, session, handler)

        worker = Worker(DummyHandler)
        worker.set_container(container)

        # Act
        task = worker.start()
        await asyncio.sleep(0.05)  # Let it run a bit
        worker.stop()
        await asyncio.sleep(0.15)  # Wait for shutdown

        # Assert - Task should complete (not be cancelled)
        assert worker.state.status == WorkerStatus.STOPPING or task.done()

        # Cleanup
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_worker_finishes_current_batch_before_stopping(self):
        """Worker should finish processing current event before stopping."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")

        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(events=[event], claimed_at=datetime.now(UTC))
        outbox.mark_delivered = AsyncMock()

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        event_processed = asyncio.Event()

        class SlowHandler(EventHandler[DummyEvent]):
            async def handle(self, event: DummyEvent) -> None:
                await asyncio.sleep(0.1)  # Simulate processing time
                event_processed.set()

        handler = SlowHandler()
        container = make_mock_container(outbox, session, handler)

        worker = Worker(SlowHandler)
        worker.set_container(container)

        # Act
        task = worker.start()
        await asyncio.sleep(0.02)  # Let it start processing
        worker.stop()

        # Wait for event to complete
        try:
            await asyncio.wait_for(event_processed.wait(), timeout=0.5)
        except asyncio.TimeoutError:
            pass

        # Assert - Event should have been processed despite stop signal
        assert event_processed.is_set()

        # Cleanup
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_worker_handles_handler_error(self):
        """Worker should handle errors in handler and mark event failed."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        event = DummyEvent(id=EventId(uuid4()), data="test")
        claim_result = ClaimResult(events=[event], claimed_at=datetime.now(UTC))

        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_failed_with_retry = AsyncMock()

        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()

        handler = FailingHandler()
        container = make_mock_container(outbox, session, handler)

        worker = Worker(FailingHandler)
        worker.set_container(container)

        # Act - Run one poll cycle
        await worker._poll_once()

        # Assert - Event should be marked as failed
        outbox.mark_failed_with_retry.assert_called_once()
        assert worker.state.failed_count == 1
        assert worker.state.error is not None
