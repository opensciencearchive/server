"""BackgroundWorker - unified background work using APScheduler."""

import logging
from collections import defaultdict
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, NewType, Union, cast
from uuid import uuid4

from apscheduler import AsyncScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dishka import AsyncContainer
from sqlalchemy.ext.asyncio import AsyncSession

from osa.application.event import ServerStarted
from osa.domain.shared.event import BatchEventListener, Event, EventId, EventListener, Schedule
from osa.domain.shared.outbox import Outbox
from osa.util.di.scope import Scope

logger = logging.getLogger(__name__)


# Type aliases for DI
# Listener can be either EventListener (single event) or BatchEventListener (batch of events)
ListenerType = Union[type[EventListener[Any]], type[BatchEventListener[Any]]]
Subscriptions = NewType("Subscriptions", list[ListenerType])


@dataclass
class ScheduleConfig:
    """Configuration for a scheduled task."""

    schedule_type: type[Schedule]
    cron: str
    id: str
    params: dict[str, Any] = field(default_factory=dict)


ScheduleConfigs = NewType("ScheduleConfigs", list[ScheduleConfig])


class BackgroundWorker:
    """Unified background worker: outbox polling + scheduled tasks.

    Uses APScheduler for all timing. APP-scoped, spawns UOW scopes per work unit.

    - Outbox polling: IntervalTrigger, dispatches events to EventListeners
    - Scheduled tasks: CronTrigger, runs Schedule.run() with params

    Usage:
        async with worker:
            # worker is running, yield to application
            ...
    """

    def __init__(
        self,
        container: AsyncContainer,
        subscriptions: Subscriptions,
        schedules: ScheduleConfigs,
        poll_interval: float = 0.5,
        batch_size: int = 100,
    ) -> None:
        self._container = container
        self._poll_interval = poll_interval
        self._batch_size = batch_size

        # Maps event type -> listener TYPE (not instance!)
        # Supports both EventListener (single) and BatchEventListener (batch)
        self._listener_types: dict[type[Event], ListenerType] = {}
        self._batch_listener_types: set[type[Event]] = set()

        for listener_type in subscriptions:
            event_type = listener_type.__event_type__
            self._listener_types[event_type] = listener_type

            # Track which event types use batch listeners
            if hasattr(listener_type, "handle_batch"):
                self._batch_listener_types.add(event_type)
                logger.debug(
                    f"Registered batch listener {listener_type.__name__} for {event_type.__name__}"
                )
            else:
                logger.debug(f"Registered {listener_type.__name__} for {event_type.__name__}")

        # Schedule configs
        self._schedules = schedules

        self._scheduler = AsyncScheduler()
        self._exit_stack: AsyncExitStack | None = None

    async def __aenter__(self) -> "BackgroundWorker":
        """Start the background worker."""
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()

        # Enter scheduler context (keeps it alive until __aexit__)
        await self._exit_stack.enter_async_context(self._scheduler)

        # Register outbox polling as interval task
        await self._scheduler.add_schedule(
            self._poll_outbox,
            IntervalTrigger(seconds=self._poll_interval),
            id="outbox-poll",
        )
        logger.debug(f"Registered outbox polling (interval={self._poll_interval}s)")

        # Register schedules as cron tasks
        for config in self._schedules:
            await self._scheduler.add_schedule(
                self._run_schedule,
                CronTrigger.from_crontab(config.cron),
                id=config.id,
                kwargs={"config": config},
            )
            logger.debug(f"Registered {config.id} (cron={config.cron})")

        await self._scheduler.start_in_background()
        logger.info(
            f"BackgroundWorker started with {len(self._listener_types)} listeners, "
            f"{len(self._schedules)} schedules"
        )

        # Emit ServerStarted to trigger startup listeners
        await self._emit_server_started()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Stop the background worker."""
        if self._exit_stack:
            await self._exit_stack.__aexit__(exc_type, exc_val, exc_tb)
        logger.info("BackgroundWorker stopped")

    async def _emit_server_started(self) -> None:
        """Emit ServerStarted event to trigger startup listeners."""
        async with self._container(scope=Scope.UOW) as scope:
            outbox = await scope.get(Outbox)
            await outbox.append(ServerStarted(id=EventId(uuid4())))
            session = await scope.get(AsyncSession)
            await session.commit()
        logger.info("ServerStarted event emitted")

    async def _poll_outbox(self) -> None:
        """Interval task: fetch pending events and dispatch.

        Events are grouped by type. Batch listeners receive all events of their
        type together, while regular listeners receive events one-by-one.
        """
        try:
            # Fetch pending events in one scope
            async with self._container(scope=Scope.UOW) as scope:
                outbox = await scope.get(Outbox)
                events = await outbox.fetch_pending(self._batch_size)
                session = await scope.get(AsyncSession)
                await session.commit()

            if not events:
                return

            # Group events by type for batch processing
            by_type: dict[type[Event], list[Event]] = defaultdict(list)
            for event in events:
                by_type[type(event)].append(event)

            # Log the distribution of event types (shows round-robin working)
            type_counts = {t.__name__: len(evts) for t, evts in by_type.items()}
            logger.info(f"Processing {len(events)} events: {type_counts}")

            # Process each event type
            for event_type, type_events in by_type.items():
                if event_type in self._batch_listener_types:
                    # Batch listener - dispatch all events together
                    await self._dispatch_batch(type_events)
                else:
                    # Regular listener - dispatch one-by-one
                    for event in type_events:
                        await self._dispatch(event)

        except Exception as e:
            # Log but don't re-raise - let the scheduler continue polling
            logger.exception(f"Error in outbox poll cycle: {e}")

    async def _dispatch(self, event: Event) -> None:
        """Dispatch a single event to its listener in UOW scope."""
        listener_type = self._listener_types.get(type(event))
        if listener_type is None:
            # No listener - mark as delivered so it doesn't stay in outbox forever
            async with self._container(scope=Scope.UOW) as scope:
                outbox = await scope.get(Outbox)
                await outbox.mark_delivered(event.id)
                session = await scope.get(AsyncSession)
                await session.commit()
            logger.debug(f"No listener for {type(event).__name__}, marked as delivered")
            return

        try:
            logger.debug(f"Dispatching {type(event).__name__} -> {listener_type.__name__}")
            async with self._container(scope=Scope.UOW) as scope:
                # Dishka creates a fresh listener instance with injected deps
                listener = cast(EventListener[Any], await scope.get(listener_type))
                await listener.handle(event)

                # Mark delivered and commit
                outbox = await scope.get(Outbox)
                await outbox.mark_delivered(event.id)
                session = await scope.get(AsyncSession)
                await session.commit()

            logger.debug(f"Delivered {type(event).__name__} (id={event.id})")

        except Exception as e:
            logger.error(f"Failed to handle {type(event).__name__} (id={event.id}): {e}")
            # Mark failed in a new scope
            async with self._container(scope=Scope.UOW) as scope:
                outbox = await scope.get(Outbox)
                await outbox.mark_failed(event.id, str(e))
                session = await scope.get(AsyncSession)
                await session.commit()

    async def _dispatch_batch(self, events: list[Event]) -> None:
        """Dispatch a batch of events to a BatchEventListener in UOW scope.

        All events in the batch are of the same type and processed together.
        On success, all events are marked delivered. On failure, all are marked failed.
        """
        if not events:
            return

        event_type = type(events[0])
        listener_type = self._listener_types.get(event_type)

        if listener_type is None:
            # No listener - mark all as delivered
            async with self._container(scope=Scope.UOW) as scope:
                outbox = await scope.get(Outbox)
                for event in events:
                    await outbox.mark_delivered(event.id)
                session = await scope.get(AsyncSession)
                await session.commit()
            logger.debug(
                f"No listener for {event_type.__name__}, marked {len(events)} as delivered"
            )
            return

        event_ids = [e.id for e in events]

        try:
            logger.debug(
                f"Dispatching batch of {len(events)} {event_type.__name__} -> {listener_type.__name__}"
            )
            async with self._container(scope=Scope.UOW) as scope:
                # Dishka creates a fresh batch listener instance with injected deps
                listener = cast(BatchEventListener[Any], await scope.get(listener_type))
                await listener.handle_batch(events)

                # Mark all as delivered and commit
                outbox = await scope.get(Outbox)
                for event_id in event_ids:
                    await outbox.mark_delivered(event_id)
                session = await scope.get(AsyncSession)
                await session.commit()

            logger.debug(f"Delivered batch of {len(events)} {event_type.__name__} events")

        except Exception as e:
            error_msg = str(e)
            logger.error(
                f"Failed to handle batch of {len(events)} {event_type.__name__} events: {error_msg}"
            )
            # Mark all as failed in a new scope
            async with self._container(scope=Scope.UOW) as scope:
                outbox = await scope.get(Outbox)
                for event_id in event_ids:
                    await outbox.mark_failed(event_id, error_msg)
                session = await scope.get(AsyncSession)
                await session.commit()

    async def _run_schedule(self, config: ScheduleConfig) -> None:
        """Cron task: run a scheduled task in UOW scope."""
        try:
            async with self._container(scope=Scope.UOW) as scope:
                schedule = await scope.get(config.schedule_type)
                await schedule.run(**config.params)
                session = await scope.get(AsyncSession)
                await session.commit()

            logger.debug(f"Ran schedule {config.id}")

        except Exception as e:
            logger.error(f"Failed to run schedule {config.id}: {e}")
