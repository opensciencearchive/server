"""Worker and WorkerPool for pull-based event processing."""

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, NewType

if TYPE_CHECKING:
    from osa.config import Config

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

    def __init__(self, handler_type: type[EventHandler[Any]], *, instance_id: int = 0) -> None:
        self._handler_type = handler_type
        self._instance_id = instance_id
        # All instances share the same consumer group so SKIP LOCKED distributes work
        self._consumer_group = handler_type.__name__

        # Read config from handler classvars
        self._event_type = handler_type.__event_type__
        self._batch_size = handler_type.__batch_size__
        self._batch_timeout = handler_type.__batch_timeout__
        self._poll_interval = handler_type.__poll_interval__
        self._max_retries = handler_type.__max_retries__
        self._claim_timeout = handler_type.__claim_timeout__

        self._config = WorkerConfig(
            name=handler_type.__name__,
            event_types=(self._event_type,),
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
        """Worker name (handler class name + instance suffix if concurrent)."""
        if self._instance_id == 0:
            return self._handler_type.__name__
        return f"{self._handler_type.__name__}-{self._instance_id}"

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

            if not result.deliveries:
                self._state.status = WorkerStatus.IDLE
                return False

            self._state.status = WorkerStatus.PROCESSING
            self._state.current_batch = result.events
            self._state.last_claim_at = result.claimed_at

            try:
                handler = await scope.get(self._handler_type)
                events = result.events

                if self._batch_size > 1:
                    await handler.handle_batch(events)
                else:
                    await handler.handle(events[0])

                # Mark all deliveries as delivered
                for delivery in result.deliveries:
                    await outbox.mark_delivered(delivery.id)

                self._state.processed_count += len(result.deliveries)

            except SkippedEvents as e:
                logger.warning(
                    f"Worker '{self.name}' skipping {len(e.event_ids)} events: {e.reason}"
                )
                skipped_set = set(e.event_ids)
                for delivery in result.deliveries:
                    if delivery.event.id in skipped_set:
                        await outbox.mark_skipped(delivery.id, e.reason)
                    else:
                        await outbox.mark_delivered(delivery.id)
                self._state.processed_count += len(result.deliveries) - len(e.event_ids)

            except Exception as e:
                self._state.failed_count += len(result.deliveries)
                self._state.error = e
                logger.error(f"Worker '{self.name}' batch failed: {e}")
                for delivery in result.deliveries:
                    await outbox.mark_failed_with_retry(
                        delivery.id,
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
        self._device_auth_cleanup_task: asyncio.Task | None = None
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

    def register(
        self,
        handler_type: type[EventHandler[Any]],
        config: "Config | None" = None,
    ) -> Worker:
        """Register an EventHandler type and create Worker(s) for it.

        Concurrency is determined by (in priority order):
        1. Config override (e.g. ``config.worker.hook_concurrency`` for RunHooks)
        2. Handler classvar ``__concurrency__``
        3. Default of 1

        Multiple workers share the same consumer group so deliveries are
        distributed across them via FOR UPDATE SKIP LOCKED.
        """
        concurrency = getattr(handler_type, "__concurrency__", 1)

        # Apply config overrides
        if config is not None:
            from osa.domain.ingest.handler.run_hooks import RunHooks

            if handler_type is RunHooks:
                concurrency = config.worker.hook_concurrency

        first_worker = None
        for i in range(concurrency):
            worker = Worker(handler_type, instance_id=i)
            if self._container is not None:
                worker.set_container(self._container)
            self._workers.append(worker)
            if first_worker is None:
                first_worker = worker
        if concurrency > 1:
            logger.debug(
                f"Registered handler '{handler_type.__name__}' with {concurrency} concurrent workers"
            )
        else:
            logger.debug(f"Registered handler '{handler_type.__name__}' as worker")
        return first_worker  # type: ignore[return-value]

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

        # Start device authorization cleanup task (every 5 minutes)
        self._device_auth_cleanup_task = asyncio.create_task(
            self._run_device_auth_cleanup(), name="device-auth-cleanup"
        )

        logger.info(
            f"WorkerPool started with {len(self._workers)} workers, {len(schedules)} schedules"
        )

    async def _build_schedules_from_conventions(self) -> list[ScheduleConfig]:
        """Query conventions with sources and build schedule configs."""
        return []

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

        if self._device_auth_cleanup_task and not self._device_auth_cleanup_task.done():
            self._device_auth_cleanup_task.cancel()
            try:
                await self._device_auth_cleanup_task
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

    async def _run_device_auth_cleanup(self) -> None:
        """Periodically delete expired device authorizations."""
        from datetime import UTC, datetime

        from osa.domain.auth.port.repository import DeviceAuthorizationRepository

        interval = 300.0  # 5 minutes
        while not self._shutdown:
            try:
                await asyncio.sleep(interval)

                if self._shutdown or self._container is None:
                    break

                async with self._container(scope=Scope.UOW, context={Identity: System()}) as scope:
                    repo = await scope.get(DeviceAuthorizationRepository)
                    count = await repo.delete_expired_before(datetime.now(UTC))
                    if count > 0:
                        logger.info(f"Cleaned up {count} expired device authorizations")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Device auth cleanup failed: {e}")
