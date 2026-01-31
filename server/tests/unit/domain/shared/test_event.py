"""Unit tests for domain event infrastructure.

Regression tests for event listener metaclass behavior.
"""

from osa.domain.shared.event import BatchEventListener, Event, EventId, EventListener


class DummyEvent(Event):
    """Test event for verifying metaclass behavior."""

    id: EventId
    data: str


class TestEventListenerMetaclass:
    """Tests for EventListener metaclass __event_type__ extraction."""

    def test_event_listener_has_event_type_set(self):
        """EventListener subclasses should have __event_type__ extracted from generic param."""

        class MyListener(EventListener[DummyEvent]):
            async def handle(self, event: DummyEvent) -> None:
                pass

        assert hasattr(MyListener, "__event_type__")
        assert MyListener.__event_type__ is DummyEvent

    def test_batch_event_listener_has_event_type_set(self):
        """BatchEventListener subclasses should have __event_type__ extracted from generic param.

        Regression test: Previously _extract_event_type only checked for 'EventListener'
        in the origin name, causing BatchEventListener subclasses to not get __event_type__.
        """

        class MyBatchListener(BatchEventListener[DummyEvent]):
            async def handle_batch(self, events: list[DummyEvent]) -> None:
                pass

        assert hasattr(MyBatchListener, "__event_type__")
        assert MyBatchListener.__event_type__ is DummyEvent

    def test_event_listener_is_dataclass(self):
        """EventListener subclasses should be automatically converted to dataclasses."""

        class ListenerWithDeps(EventListener[DummyEvent]):
            some_dep: str

            async def handle(self, event: DummyEvent) -> None:
                pass

        # Dataclass should allow instantiation with keyword args
        listener = ListenerWithDeps(some_dep="test")
        assert listener.some_dep == "test"

    def test_batch_event_listener_is_dataclass(self):
        """BatchEventListener subclasses should be automatically converted to dataclasses."""

        class BatchListenerWithDeps(BatchEventListener[DummyEvent]):
            some_dep: str

            async def handle_batch(self, events: list[DummyEvent]) -> None:
                pass

        # Dataclass should allow instantiation with keyword args
        listener = BatchListenerWithDeps(some_dep="test")
        assert listener.some_dep == "test"
