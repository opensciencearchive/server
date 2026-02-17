"""Dependency injection provider for event system."""

import logging
from typing import Any, NewType

from dishka import AsyncContainer, provide

from osa.config import Config
from osa.domain.curation.handler import AutoApproveCuration
from osa.domain.deposition.handler.return_to_draft import ReturnToDraft
from osa.domain.index.handler import FanOutToIndexBackends, KeywordIndexHandler, VectorIndexHandler
from osa.domain.record.handler import ConvertDepositionToRecord
from osa.domain.shared.event import EventHandler
from osa.domain.shared.event_log import EventLog
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.source.handler import PullFromSource, TriggerInitialSourceRun
from osa.domain.source.schedule import SourceSchedule
from osa.domain.validation.handler import ValidateDeposition
from osa.infrastructure.event.worker import (
    ScheduleConfig,
    ScheduleConfigs,
    WorkerPool,
)
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

logger = logging.getLogger(__name__)


# Type alias for handler list
HandlerTypes = NewType("HandlerTypes", list[type[EventHandler[Any]]])

# All event handlers for WorkerPool registration
HANDLERS: HandlerTypes = HandlerTypes(
    [
        # Source handlers
        TriggerInitialSourceRun,
        PullFromSource,
        # Validation handlers
        ValidateDeposition,
        # Deposition handlers
        ReturnToDraft,
        # Curation handlers
        AutoApproveCuration,
        # Record handlers
        ConvertDepositionToRecord,
        # Index handlers
        FanOutToIndexBackends,
        VectorIndexHandler,
        KeywordIndexHandler,
    ]
)


class EventProvider(Provider):
    """Provides event system components.

    Handlers, Schedules, and Outbox are UOW-scoped (fresh per unit of work).
    WorkerPool is APP-scoped singleton.
    """

    # UOW-scoped Outbox (wraps EventRepository) - write side
    @provide(scope=Scope.UOW)
    def get_outbox(self, repo: EventRepository) -> Outbox:
        return Outbox(repo)

    # UOW-scoped EventLog (wraps EventRepository) - read side
    @provide(scope=Scope.UOW)
    def get_event_log(self, repo: EventRepository) -> EventLog:
        return EventLog(repo)

    # UOW-scoped providers for handlers
    for _handler_type in HANDLERS:
        locals()[_handler_type.__name__] = provide(_handler_type, scope=Scope.UOW)

    # UOW-scoped provider for SourceSchedule
    source_schedule = provide(SourceSchedule, scope=Scope.UOW)

    @provide(scope=Scope.APP)
    def get_handler_types(self) -> HandlerTypes:
        """Return the handler types for WorkerPool registration."""
        return HANDLERS

    @provide(scope=Scope.APP)
    def get_schedule_configs(self, config: Config) -> ScheduleConfigs:
        """Build schedule configs from application config."""
        configs: list[ScheduleConfig] = []

        for source in config.sources:
            if source.schedule is None:
                continue

            configs.append(
                ScheduleConfig(
                    schedule_type=SourceSchedule,
                    cron=source.schedule.cron,
                    id=f"source-{source.name}",
                    params={
                        "source_name": source.name,
                        "limit": source.schedule.limit,
                    },
                )
            )

        return ScheduleConfigs(configs)

    @provide(scope=Scope.APP)
    def get_worker_pool(
        self,
        container: AsyncContainer,
        handler_types: HandlerTypes,
        schedules: ScheduleConfigs,
    ) -> WorkerPool:
        """WorkerPool with pull-based event handlers.

        Registers all handlers and scheduled tasks.
        Workers use the container to create scoped dependencies per operation.
        """
        pool = WorkerPool(container=container, stale_claim_interval=60.0, schedules=schedules)

        # Register all handlers
        for handler_type in handler_types:
            pool.register(handler_type)

        logger.info(f"WorkerPool created with {len(pool.workers)} workers")
        return pool
