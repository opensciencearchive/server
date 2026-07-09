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
    Delivery,
    DeliveryStatus,
    Event,
    EventHandler,
    EventId,
    WorkerStatus,
)
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.instrumentation import OutboxInstrumentation


class RecordingOutboxInstrumentation:
    """Records delivery_completed calls for assertion (no MagicMock indirection)."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, DeliveryStatus, int, float]] = []

    def delivery_completed(self, *, consumer_group, status, retry_count, duration_s) -> None:  # noqa: ANN001
        self.calls.append((consumer_group, status, retry_count, duration_s))


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
    instrumentation: RecordingOutboxInstrumentation | None = None,
):
    """Create a mock DI container that provides scoped dependencies.

    Creates a container mock that returns Outbox, AsyncSession, handler, and
    OutboxInstrumentation when called as an async context manager with scope.
    """
    if session is None:
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.rollback = AsyncMock()

    if instrumentation is None:
        instrumentation = RecordingOutboxInstrumentation()

    async def get_dependency(cls):
        """Return the appropriate dependency based on the requested class."""
        if cls == Outbox:
            return outbox
        if cls == OutboxInstrumentation:
            return instrumentation
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
        delivery_id = "delivery-abc"
        claim_result = ClaimResult(
            deliveries=[Delivery(id=delivery_id, event=event1)],
            claimed_at=datetime.now(UTC),
        )

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
        outbox.mark_delivered.assert_called_once_with(delivery_id)

    @pytest.mark.asyncio
    async def test_worker_returns_false_when_no_events(self):
        """Worker._poll_once should return False when no events are available."""
        from osa.infrastructure.event.worker import Worker

        # Arrange
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(deliveries=[], claimed_at=datetime.now(UTC))

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
        claim_result = ClaimResult(
            deliveries=[Delivery(id="del-1", event=event)],
            claimed_at=datetime.now(UTC),
        )

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
        outbox.claim.return_value = ClaimResult(deliveries=[], claimed_at=datetime.now(UTC))

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
        outbox.claim.return_value = ClaimResult(deliveries=[], claimed_at=datetime.now(UTC))

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
        outbox.claim.return_value = ClaimResult(
            deliveries=[Delivery(id="del-1", event=event)],
            claimed_at=datetime.now(UTC),
        )
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
        delivery_id = "delivery-fail"
        claim_result = ClaimResult(
            deliveries=[Delivery(id=delivery_id, event=event)],
            claimed_at=datetime.now(UTC),
        )

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

        # Assert - Event should be marked as failed using delivery_id with backoff
        outbox.mark_failed_with_retry.assert_called_once()
        call_args = outbox.mark_failed_with_retry.call_args
        assert call_args[0][0] == delivery_id
        assert call_args[0][1] == "Processing failed"
        assert call_args[1]["max_retries"] == 3
        assert call_args[1]["deliver_after"] is not None
        assert worker.state.failed_count == 1
        assert worker.state.error is not None


# Two distinct, valid, sampled W3C traceparents (flags=01).
_TP1 = "00-11111111111111111111111111111111-2222222222222222-01"
_TP2 = "00-33333333333333333333333333333333-4444444444444444-01"


class TestLinksFromDeliveries:
    """Worker._links_from_deliveries turns claimed traceparents into span links."""

    def test_valid_traceparents_become_links(self):
        from osa.infrastructure.event.worker import Worker

        worker = Worker(DummyHandler)
        d1 = Delivery(id="del-1", event=None, trace_context=_TP1)  # type: ignore[arg-type]
        d2 = Delivery(id="del-2", event=None, trace_context=_TP2)  # type: ignore[arg-type]

        links = worker._links_from_deliveries([d1, d2])

        assert len(links) == 2
        (sc1, attrs1), (sc2, attrs2) = links
        assert sc1.trace_id == int("1" * 32, 16)
        assert sc2.trace_id == int("3" * 32, 16)
        assert attrs1 == {"delivery.id": "del-1"}
        assert attrs2 == {"delivery.id": "del-2"}

    def test_malformed_traceparent_is_skipped_with_warning(self, monkeypatch):
        import osa.infrastructure.event.worker as worker_module
        from osa.infrastructure.event.worker import Worker

        fake_logger = MagicMock()
        monkeypatch.setattr(worker_module, "logger", fake_logger)

        worker = Worker(DummyHandler)
        good = Delivery(id="ok", event=None, trace_context=_TP1)  # type: ignore[arg-type]
        bad = Delivery(id="bad", event=None, trace_context="garbage")  # type: ignore[arg-type]

        links = worker._links_from_deliveries([good, bad])

        # The good one still links; the bad one is dropped, not fatal.
        assert len(links) == 1
        assert links[0][1] == {"delivery.id": "ok"}
        assert fake_logger.warn.call_count == 1

    def test_none_trace_context_skipped_silently(self, monkeypatch):
        import osa.infrastructure.event.worker as worker_module
        from osa.infrastructure.event.worker import Worker

        fake_logger = MagicMock()
        monkeypatch.setattr(worker_module, "logger", fake_logger)

        worker = Worker(DummyHandler)
        d = Delivery(id="del-1", event=None, trace_context=None)  # type: ignore[arg-type]

        links = worker._links_from_deliveries([d])

        assert links == []
        fake_logger.warn.assert_not_called()


class TestDispatchSpan:
    """Dispatch is wrapped in a worker.dispatch span whose _links come from the batch."""

    @pytest.mark.asyncio
    async def test_dispatch_span_receives_batch_links(self, monkeypatch):
        import osa.infrastructure.event.worker as worker_module
        from osa.infrastructure.event.worker import Worker

        captured: dict = {}

        class _FakeSpan:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake_span(msg, **kwargs):
            captured["msg"] = msg
            captured["kwargs"] = kwargs
            return _FakeSpan()

        monkeypatch.setattr(worker_module.logfire, "span", fake_span)

        event = DummyEvent(id=EventId(uuid4()), data="event1")
        claim_result = ClaimResult(
            deliveries=[Delivery(id="del-1", event=event, trace_context=_TP1)],
            claimed_at=datetime.now(UTC),
        )
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()

        handler = DummyHandler(processed_events=[])
        container = make_mock_container(outbox, handler=handler)

        worker = Worker(DummyHandler)
        worker.set_container(container)

        await worker._poll_once()

        assert captured["msg"] == "worker.dispatch {consumer_group}"
        assert captured["kwargs"]["consumer_group"] == "DummyHandler"
        assert captured["kwargs"]["batch_size"] == 1
        links = captured["kwargs"]["_links"]
        assert len(links) == 1
        assert links[0][1] == {"delivery.id": "del-1"}


class TestDeliveryMetrics:
    """Worker emits delivery_completed per delivery in each terminal branch."""

    @pytest.mark.asyncio
    async def test_success_emits_delivered(self):
        from osa.infrastructure.event.worker import Worker

        event = DummyEvent(id=EventId(uuid4()), data="e")
        claim_result = ClaimResult(
            deliveries=[Delivery(id="del-1", event=event, retry_count=2)],
            claimed_at=datetime.now(UTC),
        )
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()

        instrumentation = RecordingOutboxInstrumentation()
        handler = DummyHandler(processed_events=[])
        container = make_mock_container(outbox, handler=handler, instrumentation=instrumentation)

        worker = Worker(DummyHandler)
        worker.set_container(container)

        await worker._poll_once()

        assert len(instrumentation.calls) == 1
        cg, status, retry_count, duration = instrumentation.calls[0]
        assert cg == "DummyHandler"
        assert status == DeliveryStatus.DELIVERED
        assert retry_count == 2
        assert duration >= 0.0

    @pytest.mark.asyncio
    async def test_skip_emits_skipped_and_delivered(self):
        from osa.domain.shared.error import SkippedEvents
        from osa.infrastructure.event.worker import Worker

        keep = DummyEvent(id=EventId(uuid4()), data="keep")
        drop = DummyEvent(id=EventId(uuid4()), data="drop")
        claim_result = ClaimResult(
            deliveries=[
                Delivery(id="del-keep", event=keep),
                Delivery(id="del-drop", event=drop),
            ],
            claimed_at=datetime.now(UTC),
        )
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_delivered = AsyncMock()
        outbox.mark_skipped = AsyncMock()

        class SkipHandler(EventHandler[DummyEvent]):
            __batch_size__: ClassVar[int] = 10

            async def handle_batch(self, events):
                raise SkippedEvents(event_ids=[drop.id], reason="dupe")

        instrumentation = RecordingOutboxInstrumentation()
        handler = SkipHandler()
        container = make_mock_container(outbox, handler=handler, instrumentation=instrumentation)

        worker = Worker(SkipHandler)
        worker.set_container(container)

        await worker._poll_once()

        emitted = sorted(c[1].value for c in instrumentation.calls)
        assert emitted == sorted([DeliveryStatus.DELIVERED.value, DeliveryStatus.SKIPPED.value])

    @pytest.mark.asyncio
    async def test_failure_emits_failed(self):
        from osa.infrastructure.event.worker import Worker

        event = DummyEvent(id=EventId(uuid4()), data="e")
        claim_result = ClaimResult(
            deliveries=[Delivery(id="del-1", event=event)],
            claimed_at=datetime.now(UTC),
        )
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = claim_result
        outbox.mark_failed_with_retry = AsyncMock()

        instrumentation = RecordingOutboxInstrumentation()
        handler = FailingHandler()
        container = make_mock_container(outbox, handler=handler, instrumentation=instrumentation)

        worker = Worker(FailingHandler)
        worker.set_container(container)

        await worker._poll_once()

        assert len(instrumentation.calls) == 1
        assert instrumentation.calls[0][1] == DeliveryStatus.FAILED
