"""Dependency injection provider for event system."""

import logging

from dishka import AsyncContainer, provide

from osa.config import Config
from osa.domain.curation.listener import AutoApproveCurationTool
from osa.domain.index.listener import ProjectNewRecordToIndexes
from osa.domain.ingest.listener import IngestFromUpstream, TriggerInitialIngestion
from osa.domain.ingest.schedule import IngestSchedule
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
LISTENER_TYPES: Subscriptions = Subscriptions([
    TriggerInitialIngestion,
    IngestFromUpstream,
    ValidateNewDeposition,
    AutoApproveCurationTool,
    ConvertDepositionToRecord,
    ProjectNewRecordToIndexes,
])


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

    # UOW-scoped provider for IngestSchedule
    ingest_schedule = provide(IngestSchedule, scope=Scope.UOW)

    @provide(scope=Scope.APP)
    def get_subscriptions(self) -> Subscriptions:
        """Return the listener types for BackgroundWorker registration."""
        return LISTENER_TYPES

    @provide(scope=Scope.APP)
    def get_schedule_configs(self, config: Config) -> ScheduleConfigs:
        """Build schedule configs from application config."""
        configs: list[ScheduleConfig] = []

        for ingestor in config.ingestors:
            if ingestor.schedule is None:
                continue

            configs.append(
                ScheduleConfig(
                    schedule_type=IngestSchedule,
                    cron=ingestor.schedule.cron,
                    id=f"ingest-{ingestor.name}",
                    params={
                        "ingestor_name": ingestor.name,
                        "limit": ingestor.schedule.limit,
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
