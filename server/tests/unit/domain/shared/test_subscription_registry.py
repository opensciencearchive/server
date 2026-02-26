"""Unit tests for SubscriptionRegistry.

T008: Test registry builds from handler list; test event types map to correct consumer groups.
"""

from osa.domain.shared.event import Event, EventHandler, EventId
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry


class EventA(Event):
    """Test event A."""

    id: EventId


class EventB(Event):
    """Test event B."""

    id: EventId


class HandlerForA(EventHandler[EventA]):
    """Handles EventA."""

    async def handle(self, event: EventA) -> None:
        pass


class AnotherHandlerForA(EventHandler[EventA]):
    """Second handler for EventA."""

    async def handle(self, event: EventA) -> None:
        pass


class HandlerForB(EventHandler[EventB]):
    """Handles EventB."""

    async def handle(self, event: EventB) -> None:
        pass


def build_registry(handlers: list) -> SubscriptionRegistry:
    """Build a SubscriptionRegistry from a list of handler types."""
    registry: dict[str, set[str]] = {}
    for handler in handlers:
        event_type_name = handler.__event_type__.__name__
        if event_type_name not in registry:
            registry[event_type_name] = set()
        registry[event_type_name].add(handler.__name__)
    return SubscriptionRegistry(registry)


class TestSubscriptionRegistry:
    """Test SubscriptionRegistry construction from HANDLERS list."""

    def test_builds_from_handler_list(self):
        """Registry should map event type names to handler class names."""
        handlers = [HandlerForA, AnotherHandlerForA, HandlerForB]
        registry = build_registry(handlers)

        assert registry["EventA"] == {"HandlerForA", "AnotherHandlerForA"}
        assert registry["EventB"] == {"HandlerForB"}

    def test_single_handler_per_event(self):
        """An event with one handler maps to a single consumer group."""
        registry = build_registry([HandlerForB])

        assert registry["EventB"] == {"HandlerForB"}

    def test_multiple_handlers_per_event(self):
        """An event with multiple handlers maps to multiple consumer groups."""
        registry = build_registry([HandlerForA, AnotherHandlerForA])

        assert len(registry["EventA"]) == 2
        assert "HandlerForA" in registry["EventA"]
        assert "AnotherHandlerForA" in registry["EventA"]

    def test_event_not_in_registry_returns_empty(self):
        """Querying an unregistered event type returns KeyError (use .get())."""
        registry = build_registry([HandlerForA])

        assert registry.get("NonExistentEvent", set()) == set()

    def test_empty_handler_list(self):
        """Empty handler list produces empty registry."""
        registry = build_registry([])

        assert dict(registry) == {}
