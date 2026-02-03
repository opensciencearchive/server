"""Unit tests for domain event infrastructure.

Tests for event handler metaclass behavior.
"""

from osa.domain.shared.event import Event, EventHandler, EventId


class DummyEvent(Event):
    """Test event for verifying metaclass behavior."""

    id: EventId
    data: str


class TestEventHandlerMetaclass:
    """Tests for EventHandler metaclass __event_type__ extraction."""

    def test_event_handler_has_event_type_set(self):
        """EventHandler subclasses should have __event_type__ extracted from generic param."""

        class MyHandler(EventHandler[DummyEvent]):
            async def handle(self, event: DummyEvent) -> None:
                pass

        assert hasattr(MyHandler, "__event_type__")
        assert MyHandler.__event_type__ is DummyEvent

    def test_event_handler_is_dataclass(self):
        """EventHandler subclasses should be automatically converted to dataclasses."""

        class HandlerWithDeps(EventHandler[DummyEvent]):
            some_dep: str

            async def handle(self, event: DummyEvent) -> None:
                pass

        # Dataclass should allow instantiation with keyword args
        handler = HandlerWithDeps(some_dep="test")
        assert handler.some_dep == "test"

    def test_event_handler_default_classvars(self):
        """EventHandler should have sensible default classvars."""

        class MyHandler(EventHandler[DummyEvent]):
            async def handle(self, event: DummyEvent) -> None:
                pass

        assert MyHandler.__routing_key__ is None
        assert MyHandler.__batch_size__ == 1
        assert MyHandler.__batch_timeout__ == 5.0
        assert MyHandler.__poll_interval__ == 0.5
        assert MyHandler.__max_retries__ == 3
        assert MyHandler.__claim_timeout__ == 300.0

    def test_event_handler_custom_classvars(self):
        """EventHandler subclasses can override classvars."""

        class BatchHandler(EventHandler[DummyEvent]):
            __routing_key__ = "my-queue"
            __batch_size__ = 100
            __batch_timeout__ = 10.0

            async def handle_batch(self, events: list[DummyEvent]) -> None:
                pass

        assert BatchHandler.__routing_key__ == "my-queue"
        assert BatchHandler.__batch_size__ == 100
        assert BatchHandler.__batch_timeout__ == 10.0
