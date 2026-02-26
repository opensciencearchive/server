"""Worker and WorkerPool for pull-based event processing."""

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, NewType

from apscheduler import AsyncScheduler
from apscheduler.triggers.cron import CronTrigger
from dishka import AsyncContainer
from osa.domain.auth.model.identity import Identity, System
from osa.domain.shared.error import SkippedEvents
from osa.domain.shared.event import (
    EventHandler,
    Schedule,
    WorkerConfig,
    WorkerState,
    WorkerStatus,
)
from osa.domain.shared.outbox import Outbox
from osa.util.di.scope import Scope

logger = logging.getLogger(__name__)


@dataclass
class ScheduleConfig:
    """Configuration for a scheduled task."""

    schedule_type: type[Schedule]
    cron: str
    id: str
    params: dict[str, Any] = field(default_factory=dict)


ScheduleConfigs = NewType("ScheduleConfigs", list[ScheduleConfig])


class Worker:
    """Pull-based event worker that delegates to an EventHandler.

    Each Worker is bound to a single EventHandler type and uses the handler's
    class name as its consumer_group. Deliveries are claimed per consumer group,
    enabling multiple handlers to independently process the same event.
    """

    def __init__(self, handler_type: type[EventHandler[Any]]) -> None:
        self._handler_type = handler_type
        self._consumer_group = handler_type.__name__

        # Read config from handler classvars
        self._event_type = handler_type.__event_type__
        self._routing_key = handler_type.__routing_key__
        self._batch_size = handler_type.__batch_size__
        self._batch_timeout = handler_type.__batch_timeout__
        self._poll_interval = handler_type.__poll_interval__
        self._max_retries = handler_type.__max_retries__
        self._claim_timeout = handler_type.__claim_timeout__

        self._config = WorkerConfig(
            name=handler_type.__name__,
            event_types=(self._event_type,),
            routing_key=self._routing_key,
            batch_size=self._batch_size,
            batch_timeout=self._batch_timeout,
            poll_interval=self._poll_interval,
            max_retries=self._max_retries,
            claim_timeout=self._claim_timeout,
        )
        self._state = WorkerState(config=self._config)
        self._shutdown = False
        self._task: asyncio.Task | None = None
        self._container: AsyncContainer | None = None

    @property
    def name(self) -> str:
        """Worker name (handler class name)."""
        return self._handler_type.__name__

    @property
    def consumer_group(self) -> str:
        """Consumer group name for delivery claiming."""
        return self._consumer_group

    @property
    def handler_type(self) -> type[EventHandler[Any]]:
        """The EventHandler type this worker delegates to."""
        return self._handler_type

    @property
    def config(self) -> WorkerConfig:
        """Worker configuration (derived from handler classvars)."""
        return self._config

    @property
    def state(self) -> WorkerState:
        """Current worker state."""
        return self._state

    def set_container(self, container: AsyncContainer) -> None:
        """Set the DI container for scoped dependency resolution."""
        self._container = container

    def start(self) -> asyncio.Task:
        """Start the worker in a background task."""
        if self._container is None:
            raise RuntimeError("Container not set. Call set_container() first.")

        self._shutdown = False
        self._task = asyncio.create_task(self._run(), name=f"worker-{self.name}")
        logger.info(f"Worker '{self.name}' started")
        return self._task

    def stop(self) -> None:
        """Signal the worker to stop gracefully."""
        self._shutdown = True
        self._state.status = WorkerStatus.STOPPING
        logger.info(f"Worker '{self.name}' stopping...")

    async def _run(self) -> None:
        """Main worker loop."""
        try:
            while not self._shutdown:
                had_events = await self._poll_once()
                if not had_events:
                    await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            logger.info(f"Worker '{self.name}' cancelled")
            raise
        except Exception as e:
            logger.exception(f"Worker '{self.name}' crashed: {e}")
            self._state.error = e
            raise
        finally:
            logger.info(f"Worker '{self.name}' stopped")

    async def _poll_once(self) -> bool:
        """Execute one poll cycle: claim deliveries, process, mark status.

        Returns:
            True if events were processed, False if idle.
        """
        if self._container is None:
            raise RuntimeError("Container not set")

        self._state.status = WorkerStatus.CLAIMING

        async with self._container(scope=Scope.UOW, context={Identity: System()}) as scope:
            outbox = await scope.get(Outbox)

            # Claim deliveries for this consumer group
            result = await outbox.claim(
                event_types=[self._event_type],
                limit=self._batch_size,
                consumer_group=self._consumer_group,
            )

            if not result.events:
                self._state.status = WorkerStatus.IDLE
                return False

            self._state.status = WorkerStatus.PROCESSING
            self._state.current_batch = result.events
            self._state.last_claim_at = result.claimed_at

            try:
                handler = await scope.get(self._handler_type)

                if self._batch_size > 1:
                    await handler.handle_batch(result.events)
                else:
                    await handler.handle(result.events[0])

                # Mark all deliveries as delivered (using delivery_id)
                for event in result.events:
                    delivery_id = getattr(event, "_delivery_id", str(event.id))
                    await outbox.mark_delivered(delivery_id)

                self._state.processed_count += len(result.events)

            except SkippedEvents as e:
                logger.warning(
                    f"Worker '{self.name}' skipping {len(e.event_ids)} events: {e.reason}"
                )
                skipped_set = set(e.event_ids)
                for event in result.events:
                    delivery_id = getattr(event, "_delivery_id", str(event.id))
                    if event.id in skipped_set:
                        await outbox.mark_skipped(delivery_id, e.reason)
                    else:
                        await outbox.mark_delivered(delivery_id)
                self._state.processed_count += len(result.events) - len(e.event_ids)

            except Exception as e:
                self._state.failed_count += len(result.events)
                self._state.error = e
                logger.error(f"Worker '{self.name}' batch failed: {e}")
                for event in result.events:
                    delivery_id = getattr(event, "_delivery_id", str(event.id))
                    await outbox.mark_failed_with_retry(
                        delivery_id,
                        str(e),
                        max_retries=self._max_retries,
                    )

            finally:
                self._state.current_batch = []
                self._state.status = WorkerStatus.IDLE

        return True


class WorkerPool:
    """Manages multiple workers, scheduled tasks, and handles stale claim cleanup."""

    def __init__(
        self,
        container: AsyncContainer | None = None,
        stale_claim_interval: float = 60.0,
    ) -> None:
        self._container = container
        self._workers: list[Worker] = []
        self._stale_claim_interval = stale_claim_interval
        self._stale_claim_task: asyncio.Task | None = None
        self._shutdown = False
        self._scheduler: AsyncScheduler | None = None
        self._exit_stack: AsyncExitStack | None = None
        self._schedule_failures: dict[str, int] = {}

    def set_container(self, container: AsyncContainer) -> None:
        """Set the DI container for all workers."""
        self._container = container
        for worker in self._workers:
            worker.set_container(container)

    @property
    def workers(self) -> list[Worker]:
        """List of managed workers."""
        return self._workers

    def register(self, handler_type: type[EventHandler[Any]]) -> Worker:
        """Register an EventHandler type and create a Worker for it."""
        worker = Worker(handler_type)
        if self._container is not None:
            worker.set_container(self._container)
        self._workers.append(worker)
        logger.debug(f"Registered handler '{handler_type.__name__}' as worker")
        return worker

    def add_worker(self, worker: Worker) -> None:
        """Add a worker to the pool."""
        if self._container is not None:
            worker.set_container(self._container)
        self._workers.append(worker)
        logger.debug(f"Added worker '{worker.name}' to pool")

    def get_worker(self, name: str) -> Worker | None:
        """Get a worker by name."""
        for worker in self._workers:
            if worker.name == name:
                return worker
        return None

    async def start(self) -> None:
        """Start all workers, scheduled tasks, and the stale claim cleanup task."""
        if self._container is None:
            raise RuntimeError("Container not set. Call set_container() first.")

        self._shutdown = False

        for worker in self._workers:
            if worker._container is None:
                worker.set_container(self._container)

        # Setup scheduler for cron tasks
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()

        self._scheduler = AsyncScheduler()
        await self._exit_stack.enter_async_context(self._scheduler)

        # Build schedules dynamically from conventions with sources
        schedules = await self._build_schedules_from_conventions()
        for config in schedules:
            await self._scheduler.add_schedule(
                self._run_schedule,
                CronTrigger.from_crontab(config.cron),
                id=config.id,
                kwargs={"config": config},
            )
            logger.debug(f"Registered schedule {config.id} (cron={config.cron})")

        await self._scheduler.start_in_background()

        # Start all workers
        for worker in self._workers:
            worker.start()

        # Start stale claim cleanup task
        if self._stale_claim_interval > 0:
            self._stale_claim_task = asyncio.create_task(
                self._run_stale_claim_cleanup(), name="stale-claim-cleanup"
            )

        logger.info(
            f"WorkerPool started with {len(self._workers)} workers, {len(schedules)} schedules"
        )

    async def _build_schedules_from_conventions(self) -> list[ScheduleConfig]:
        """Query conventions with sources and build schedule configs."""
        if self._container is None:
            return []

        from osa.domain.deposition.service.convention import ConventionService
        from osa.domain.source.schedule import SourceSchedule as SourceScheduleType

        configs: list[ScheduleConfig] = []
        try:
            async with self._container(scope=Scope.UOW, context={Identity: System()}) as scope:
                convention_service = await scope.get(ConventionService)
                conventions = await convention_service.list_conventions_with_source()

                for conv in conventions:
                    if conv.source is None or conv.source.schedule is None:
                        continue
                    configs.append(
                        ScheduleConfig(
                            schedule_type=SourceScheduleType,
                            cron=conv.source.schedule.cron,
                            id=f"source-{conv.srn}",
                            params={
                                "convention": str(conv.srn),
                                "limit": conv.source.schedule.limit,
                            },
                        )
                    )
        except Exception as e:
            logger.warning(f"Failed to build schedules from conventions: {e}")

        return configs

    async def stop(self, timeout: float = 30.0) -> None:
        """Stop all workers gracefully."""
        self._shutdown = True

        for worker in self._workers:
            worker.stop()

        if self._stale_claim_task and not self._stale_claim_task.done():
            self._stale_claim_task.cancel()
            try:
                await self._stale_claim_task
            except asyncio.CancelledError:
                pass

        tasks = [w._task for w in self._workers if w._task and not w._task.done()]
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            for task in pending:
                task.cancel()

        if self._exit_stack:
            await self._exit_stack.__aexit__(None, None, None)
            self._exit_stack = None

        logger.info("WorkerPool stopped")

    async def _run_schedule(self, config: "ScheduleConfig") -> None:
        """Cron task: run a scheduled task in UOW scope."""
        if self._container is None:
            return

        try:
            async with self._container(scope=Scope.UOW, context={Identity: System()}) as scope:
                schedule = await scope.get(config.schedule_type)
                await schedule.run(**config.params)

            self._schedule_failures.pop(config.id, None)
            logger.debug(f"Ran schedule {config.id}")

        except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
            raise
        except Exception as e:
            failures = self._schedule_failures.get(config.id, 0) + 1
            self._schedule_failures[config.id] = failures
            logger.error(f"Failed to run schedule {config.id} (failures: {failures}): {e}")
            if failures >= 5:
                logger.critical(f"Schedule {config.id} has failed {failures} consecutive times")

    async def __aenter__(self) -> "WorkerPool":
        """Start the pool as async context manager."""
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # noqa: ANN001
        """Stop the pool on context exit."""
        await self.stop()

    async def _run_stale_claim_cleanup(self) -> None:
        """Periodically reset stale deliveries."""
        while not self._shutdown:
            try:
                await asyncio.sleep(self._stale_claim_interval)

                if self._shutdown or self._container is None:
                    break

                if self._workers:
                    max_timeout = max(w.config.claim_timeout for w in self._workers)

                    async with self._container(
                        scope=Scope.UOW, context={Identity: System()}
                    ) as scope:
                        outbox = await scope.get(Outbox)
                        count = await outbox.reset_stale_claims(max_timeout)
                        if count > 0:
                            logger.info(f"Reset {count} stale deliveries")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stale claim cleanup failed: {e}")
