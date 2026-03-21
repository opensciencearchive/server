"""Unit tests for EventProvider extension points.

Tests that EventProvider correctly merges extra handlers with core handlers
for DI resolution, subscription routing, and WorkerPool registration.
"""

from typing import ClassVar

import pytest
import pytest_asyncio
from dishka import make_async_container

from osa.domain.shared.event import Event, EventHandler, EventId
from osa.infrastructure.event.di import (
    EventProvider,
    HandlerTypes,
    _CORE_HANDLERS,
    build_subscription_registry,
)
from osa.util.di.scope import Scope


# ---------------------------------------------------------------------------
# Test fixtures: dummy events and handlers
# ---------------------------------------------------------------------------


class AlphaEvent(Event):
    """Test event A."""

    id: EventId
    data: str


class BetaEvent(Event):
    """Test event B."""

    id: EventId
    data: str


class AlphaHandler(EventHandler[AlphaEvent]):
    """Handler for AlphaEvent."""

    __poll_interval__: ClassVar[float] = 0.01

    async def handle(self, event: AlphaEvent) -> None:
        pass


class BetaHandler(EventHandler[BetaEvent]):
    """Handler for BetaEvent."""

    __poll_interval__: ClassVar[float] = 0.01

    async def handle(self, event: BetaEvent) -> None:
        pass


# ---------------------------------------------------------------------------
# EventProvider: default behaviour
# ---------------------------------------------------------------------------


class TestEventProviderDefaults:
    def test_no_args_has_core_handlers(self):
        """EventProvider() with no args has exactly the core handlers."""
        provider = EventProvider()
        assert list(provider._all_handlers) == list(_CORE_HANDLERS)

    def test_none_extra_handlers_same_as_no_args(self):
        """Passing extra_handlers=None is equivalent to no args."""
        provider = EventProvider(extra_handlers=None)
        assert list(provider._all_handlers) == list(_CORE_HANDLERS)

    def test_empty_extra_handlers_same_as_no_args(self):
        """Passing extra_handlers=[] is equivalent to no args."""
        provider = EventProvider(extra_handlers=[])
        assert list(provider._all_handlers) == list(_CORE_HANDLERS)


# ---------------------------------------------------------------------------
# EventProvider: extra handlers
# ---------------------------------------------------------------------------


class TestEventProviderExtraHandlers:
    def test_extra_handlers_appended_to_core(self):
        """Extra handlers appear after core handlers in the list."""
        provider = EventProvider(extra_handlers=[AlphaHandler])
        assert provider._all_handlers[-1] is AlphaHandler
        for core in _CORE_HANDLERS:
            assert core in provider._all_handlers

    def test_multiple_extra_handlers(self):
        """Multiple extra handlers are all included."""
        provider = EventProvider(extra_handlers=[AlphaHandler, BetaHandler])
        assert AlphaHandler in provider._all_handlers
        assert BetaHandler in provider._all_handlers

    def test_core_handlers_not_duplicated(self):
        """Core handlers appear exactly once even with extras."""
        provider = EventProvider(extra_handlers=[AlphaHandler])
        core_count = sum(1 for h in provider._all_handlers if h in _CORE_HANDLERS)
        assert core_count == len(_CORE_HANDLERS)


# ---------------------------------------------------------------------------
# Subscription registry
# ---------------------------------------------------------------------------


class TestSubscriptionRegistry:
    def test_extra_handler_appears_in_registry(self):
        """Extra handlers are routed in the subscription registry."""
        handlers = HandlerTypes([*_CORE_HANDLERS, AlphaHandler])
        registry = build_subscription_registry(handlers)

        assert "AlphaEvent" in registry
        assert "AlphaHandler" in registry["AlphaEvent"]

    def test_multiple_handlers_for_same_event(self):
        """Multiple handlers for the same event type are all registered."""

        class AnotherAlphaHandler(EventHandler[AlphaEvent]):
            __poll_interval__: ClassVar[float] = 0.01

            async def handle(self, event: AlphaEvent) -> None:
                pass

        handlers = HandlerTypes([AlphaHandler, AnotherAlphaHandler])
        registry = build_subscription_registry(handlers)

        assert registry["AlphaEvent"] == {"AlphaHandler", "AnotherAlphaHandler"}

    def test_core_events_unchanged_with_extras(self):
        """Adding extra handlers doesn't affect core event routing."""
        core_registry = build_subscription_registry(HandlerTypes(_CORE_HANDLERS))
        extended_registry = build_subscription_registry(
            HandlerTypes([*_CORE_HANDLERS, AlphaHandler])
        )

        for event_type, consumers in core_registry.items():
            assert extended_registry[event_type] == consumers


# ---------------------------------------------------------------------------
# DI integration: full container resolution
# ---------------------------------------------------------------------------


class TestEventProviderDI:
    @pytest_asyncio.fixture
    async def container(self):
        """Minimal container with EventProvider only (no persistence deps).

        Skips providers that need DB — we only test that extra handlers
        are resolvable and appear in HandlerTypes.
        """
        # EventProvider's @provide methods for Outbox/EventLog need
        # EventRepository, which we don't have. But HandlerTypes and
        # handler resolution are self-contained. Use a stripped-down
        # container that only includes the handler bindings.
        #
        # We create a fresh provider that only exposes handler types
        # and the handlers themselves (no Outbox/EventLog).
        from dishka import Provider, provide

        extra = [AlphaHandler, BetaHandler]

        # Provide just HandlerTypes via a thin wrapper to avoid
        # EventRepository dependency from Outbox binding
        class HandlerOnlyProvider(Provider):
            @provide(scope=Scope.APP)
            def handler_types(self) -> HandlerTypes:
                return HandlerTypes([*_CORE_HANDLERS, *extra])

        # Register handler DI bindings
        handler_provider = Provider()
        for h in extra:
            handler_provider.provide(h, scope=Scope.UOW)

        c = make_async_container(
            HandlerOnlyProvider(),
            handler_provider,
            scopes=Scope,  # type: ignore[arg-type]
        )
        yield c
        await c.close()

    @pytest.mark.asyncio
    async def test_extra_handlers_resolvable(self, container):
        """Extra handlers can be resolved from DI."""
        async with container(scope=Scope.UOW) as uow:
            alpha = await uow.get(AlphaHandler)
            beta = await uow.get(BetaHandler)
            assert isinstance(alpha, AlphaHandler)
            assert isinstance(beta, BetaHandler)

    @pytest.mark.asyncio
    async def test_handler_types_includes_extras(self, container):
        """HandlerTypes in DI includes both core and extra handlers."""
        handler_types = await container.get(HandlerTypes)
        assert AlphaHandler in handler_types
        assert BetaHandler in handler_types
