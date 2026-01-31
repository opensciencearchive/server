"""Dependency injection provider for event system."""

import logging

from dishka import AsyncContainer, provide

from osa.config import Config
from osa.domain.curation.listener import AutoApproveCurationTool
from osa.domain.index.listener import FlushIndexesOnSourceComplete, ProjectNewRecordToIndexes
from osa.domain.source.listener import PullFromSource, TriggerInitialSourceRun
from osa.domain.source.schedule import SourceSchedule
from osa.domain.record.listener import ConvertDepositionToRecord
from osa.domain.shared.event_log import EventLog
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.event_repository import EventRepository
from osa.domain.validation.listener import ValidateNewDeposition
from osa.infrastructure.event.worker import (
    BackgroundWorker,
    ScheduleConfig,
    ScheduleConfigs,
    Subscriptions,
)
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

logger = logging.getLogger(__name__)


# All event listeners - single source of truth
LISTENER_TYPES: Subscriptions = Subscriptions(
    [
        TriggerInitialSourceRun,
        PullFromSource,
        ValidateNewDeposition,
        AutoApproveCurationTool,
        ConvertDepositionToRecord,
        ProjectNewRecordToIndexes,
        FlushIndexesOnSourceComplete,
    ]
)


class EventProvider(Provider):
    """Provides event system components.

    Listeners, Schedules, and Outbox are UOW-scoped (fresh per unit of work).
    BackgroundWorker is APP-scoped singleton.
    """

    # UOW-scoped Outbox (wraps EventRepository) - write side
    @provide(scope=Scope.UOW)
    def get_outbox(self, repo: EventRepository) -> Outbox:
        return Outbox(repo)

    # UOW-scoped EventLog (wraps EventRepository) - read side
    @provide(scope=Scope.UOW)
    def get_event_log(self, repo: EventRepository) -> EventLog:
        return EventLog(repo)

    # UOW-scoped providers for listeners
    for _listener_type in LISTENER_TYPES:
        locals()[_listener_type.__name__] = provide(_listener_type, scope=Scope.UOW)

    # UOW-scoped provider for SourceSchedule
    source_schedule = provide(SourceSchedule, scope=Scope.UOW)

    @provide(scope=Scope.APP)
    def get_subscriptions(self) -> Subscriptions:
        """Return the listener types for BackgroundWorker registration."""
        return LISTENER_TYPES

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
    def get_background_worker(
        self,
        container: AsyncContainer,
        subscriptions: Subscriptions,
        schedules: ScheduleConfigs,
    ) -> BackgroundWorker:
        """BackgroundWorker that polls outbox and runs scheduled tasks."""
        return BackgroundWorker(container, subscriptions, schedules)
