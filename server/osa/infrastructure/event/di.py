"""Dependency injection provider for event system."""

import logging
from typing import Any, NewType

from dishka import AsyncContainer, provide

from osa.domain.curation.handler import AutoApproveCuration
from osa.domain.deposition.handler import CreateDepositionFromSource, ReturnToDraft
from osa.domain.feature.handler import CreateFeatureTables, InsertRecordFeatures
from osa.domain.record.handler import ConvertDepositionToRecord
from osa.domain.shared.event import EventHandler
from osa.domain.shared.event_log import EventLog
from osa.domain.shared.model.subscription_registry import SubscriptionRegistry
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.source.handler import PullFromSource, TriggerInitialSourceRun
from osa.domain.source.schedule import SourceSchedule
from osa.domain.validation.handler import ValidateDeposition
from osa.infrastructure.event.worker import WorkerPool
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

logger = logging.getLogger(__name__)


# Type alias for handler list
HandlerTypes = NewType("HandlerTypes", list[type[EventHandler[Any]]])

# Core event handlers shipped with OSA
_CORE_HANDLERS: list[type[EventHandler[Any]]] = [
    # Feature handlers (must run before source triggers)
    CreateFeatureTables,
    InsertRecordFeatures,
    # Source handlers
    TriggerInitialSourceRun,
    PullFromSource,
    # Validation handlers
    ValidateDeposition,
    # Deposition handlers
    CreateDepositionFromSource,
    ReturnToDraft,
    # Curation handlers
    AutoApproveCuration,
    # Record handlers
    ConvertDepositionToRecord,
]


def build_subscription_registry(handlers: HandlerTypes) -> SubscriptionRegistry:
    """Build a SubscriptionRegistry from handler list.

    Maps each handler's __event_type__.__name__ → handler.__name__.
    """
    registry: dict[str, set[str]] = {}
    for handler in handlers:
        event_type_name = handler.__event_type__.__name__
        if event_type_name not in registry:
            registry[event_type_name] = set()
        registry[event_type_name].add(handler.__name__)
    return SubscriptionRegistry(registry)


class EventProvider(Provider):
    """Provides event system components.

    Handlers, Schedules, and Outbox are UOW-scoped (fresh per unit of work).
    WorkerPool and SubscriptionRegistry are APP-scoped singletons.

    To register additional event handlers (e.g. from an external package),
    pass them to the constructor::

        EventProvider(extra_handlers=[MeterUsage, SendNotification])

    Extra handlers are merged with the core handlers for subscription
    routing, WorkerPool registration, and DI resolution.
    """

    def __init__(
        self,
        *,
        extra_handlers: list[type[EventHandler[Any]]] | None = None,
    ) -> None:
        super().__init__()
        self._all_handlers = HandlerTypes([*_CORE_HANDLERS, *(extra_handlers or [])])

        # Register DI bindings for every handler (core + extra).
        # Each handler becomes a UOW-scoped dependency that Dishka can
        # instantiate with its declared fields injected.
        for handler_type in self._all_handlers:
            self.provide(handler_type, scope=Scope.UOW)

    # UOW-scoped Outbox (wraps EventRepository + SubscriptionRegistry)
    @provide(scope=Scope.UOW)
    def get_outbox(self, repo: EventRepository, registry: SubscriptionRegistry) -> Outbox:
        return Outbox(repo, registry)

    # UOW-scoped EventLog (wraps EventRepository) - read side
    @provide(scope=Scope.UOW)
    def get_event_log(self, repo: EventRepository) -> EventLog:
        return EventLog(repo)

    # UOW-scoped provider for SourceSchedule
    source_schedule = provide(SourceSchedule, scope=Scope.UOW)

    @provide(scope=Scope.APP)
    def get_handler_types(self) -> HandlerTypes:
        """Return all handler types (core + extra) for WorkerPool registration."""
        return self._all_handlers

    @provide(scope=Scope.APP)
    def get_subscription_registry(self, handler_types: HandlerTypes) -> SubscriptionRegistry:
        """Build subscription registry from handler list at startup."""
        registry = build_subscription_registry(handler_types)
        logger.info(
            f"Built subscription registry: {len(registry)} event types, "
            f"{sum(len(v) for v in registry.values())} consumer groups"
        )
        return registry

    @provide(scope=Scope.APP)
    def get_worker_pool(
        self,
        container: AsyncContainer,
        handler_types: HandlerTypes,
    ) -> WorkerPool:
        """WorkerPool with pull-based event handlers."""
        pool = WorkerPool(container=container, stale_claim_interval=60.0)

        for handler_type in handler_types:
            pool.register(handler_type)

        logger.info(f"WorkerPool created with {len(pool.workers)} workers")
        return pool
