"""Worker and WorkerPool for pull-based event processing."""

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any, NewType
from uuid import uuid4

from apscheduler import AsyncScheduler
from apscheduler.triggers.cron import CronTrigger
from dishka import AsyncContainer
from osa.application.event import ServerStarted
from osa.domain.auth.model.identity import Identity, System
from osa.domain.shared.error import SkippedEvents
from osa.domain.shared.event import (
    EventHandler,
    EventId,
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

    Workers claim events from the outbox using FOR UPDATE SKIP LOCKED,
    enabling concurrent processing without coordination. The Worker
    handles all polling/transaction logic while the EventHandler
    contains the business logic.

    Configuration is read from the handler's class variables:
        __event_type__: Event type to claim
        __routing_key__: Optional routing key filter
        __batch_size__: Max events per batch
        __batch_timeout__: Timeout for partial batches
        __poll_interval__: Seconds between polls when idle
        __max_retries__: Max retry attempts before marking failed
        __claim_timeout__: Seconds before claim considered stale

    Example:
        class VectorIndexHandler(EventHandler[IndexRecord]):
            __routing_key__ = "vector"
            __batch_size__ = 100

            _backend: VectorStorageBackend

            async def handle_batch(self, events: list[IndexRecord]) -> None:
                records = [(str(e.record_srn), e.metadata) for e in events]
                await self._backend.ingest_batch(records)

        # Worker created from handler type
        worker = Worker(VectorIndexHandler)
        worker.set_container(container)
        worker.start()
    """

    def __init__(self, handler_type: type[EventHandler[Any]]) -> None:
        """Initialize worker from handler type.

        Args:
            handler_type: EventHandler subclass with config in classvars.
        """
        self._handler_type = handler_type

        # Read config from handler classvars
        self._event_type = handler_type.__event_type__
        self._routing_key = handler_type.__routing_key__
        self._batch_size = handler_type.__batch_size__
        self._batch_timeout = handler_type.__batch_timeout__
        self._poll_interval = handler_type.__poll_interval__
        self._max_retries = handler_type.__max_retries__
        self._claim_timeout = handler_type.__claim_timeout__

        # Create WorkerConfig for state tracking (backwards compat)
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
        """Start the worker in a background task.

        Returns:
            The asyncio.Task running the worker.
        """
        if self._container is None:
            raise RuntimeError("Container not set. Call set_container() first.")

        self._shutdown = False
        self._task = asyncio.create_task(self._run(), name=f"worker-{self.name}")
        logger.info(f"Worker '{self.name}' started")
        return self._task

    def stop(self) -> None:
        """Signal the worker to stop gracefully.

        The worker will finish processing its current batch before stopping.
        """
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
        """Execute one poll cycle: claim, process, repeat within UOW scope.

        Returns:
            True if events were processed, False if idle.
        """
        if self._container is None:
            raise RuntimeError("Container not set")

        self._state.status = WorkerStatus.CLAIMING

        # Claim and process within a UOW scope (System identity for workers)
        async with self._container(scope=Scope.UOW, context={Identity: System()}) as scope:
            outbox = await scope.get(Outbox)

            # Claim events
            result = await outbox.claim(
                event_types=[self._event_type],
                limit=self._batch_size,
                routing_key=self._routing_key,
            )

            if not result.events:
                self._state.status = WorkerStatus.IDLE
                return False

            # Process claimed events via handler
            self._state.status = WorkerStatus.PROCESSING
            self._state.current_batch = result.events
            self._state.last_claim_at = result.claimed_at

            try:
                # Get handler instance from DI container
                handler = await scope.get(self._handler_type)

                # Delegate to handler's batch method
                if self._batch_size > 1:
                    await handler.handle_batch(result.events)
                else:
                    await handler.handle(result.events[0])

                # Mark all events as delivered
                for event in result.events:
                    await outbox.mark_delivered(event.id)

                self._state.processed_count += len(result.events)

            except SkippedEvents as e:
                # Mark specific events as skipped (not the whole batch)
                logger.warning(
                    f"Worker '{self.name}' skipping {len(e.event_ids)} events: {e.reason}"
                )
                for event_id in e.event_ids:
                    await outbox.mark_skipped(event_id, e.reason)
                # Mark remaining events as delivered
                skipped_set = set(e.event_ids)
                for event in result.events:
                    if event.id not in skipped_set:
                        await outbox.mark_delivered(event.id)
                self._state.processed_count += len(result.events) - len(e.event_ids)

            except Exception as e:
                self._state.failed_count += len(result.events)
                self._state.error = e
                logger.error(f"Worker '{self.name}' batch failed: {e}")
                # Mark all as failed with retry
                for event in result.events:
                    await outbox.mark_failed_with_retry(
                        event.id,
                        str(e),
                        max_retries=self._max_retries,
                    )

            finally:
                self._state.current_batch = []
                self._state.status = WorkerStatus.IDLE

        return True


class WorkerPool:
    """Manages multiple workers, scheduled tasks, and handles stale claim cleanup.

    Usage with handler types (preferred):
        pool = WorkerPool(container)
        pool.register(VectorIndexHandler)
        pool.register(KeywordIndexHandler)

        async with pool:
            # Workers are running
            await some_long_running_task()
        # Workers are stopped

    Legacy usage with Worker instances (deprecated):
        pool = WorkerPool(container)
        pool.add_worker(VectorIndexWorker(config, backend))
    """

    def __init__(
        self,
        container: AsyncContainer | None = None,
        stale_claim_interval: float = 60.0,
        schedules: "ScheduleConfigs | None" = None,
    ) -> None:
        self._container = container
        self._workers: list[Worker] = []
        self._stale_claim_interval = stale_claim_interval
        self._stale_claim_task: asyncio.Task | None = None
        self._shutdown = False
        self._schedules = schedules or ScheduleConfigs([])
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
        """Register an EventHandler type and create a Worker for it.

        This is the preferred way to add handlers to the pool.
        The Worker is created internally and configured from handler classvars.

        Args:
            handler_type: EventHandler subclass to register.

        Returns:
            The created Worker instance.
        """
        worker = Worker(handler_type)
        if self._container is not None:
            worker.set_container(self._container)
        self._workers.append(worker)
        logger.debug(f"Registered handler '{handler_type.__name__}' as worker")
        return worker

    def add_worker(self, worker: Worker) -> None:
        """Add a worker to the pool.

        DEPRECATED: Use register() with EventHandler types instead.
        """
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

        # Ensure all workers have the container
        for worker in self._workers:
            if worker._container is None:
                worker.set_container(self._container)

        # Setup scheduler for cron tasks
        self._exit_stack = AsyncExitStack()
        await self._exit_stack.__aenter__()

        self._scheduler = AsyncScheduler()
        await self._exit_stack.enter_async_context(self._scheduler)

        # Register schedules as cron tasks
        for config in self._schedules:
            await self._scheduler.add_schedule(
                self._run_schedule,
                CronTrigger.from_crontab(config.cron),
                id=config.id,
                kwargs={"config": config},
            )
            logger.debug(f"Registered schedule {config.id} (cron={config.cron})")

        await self._scheduler.start_in_background()

        # Emit ServerStarted event to trigger startup handlers
        await self._emit_server_started()

        # Start all workers
        for worker in self._workers:
            worker.start()

        # Start stale claim cleanup task
        if self._stale_claim_interval > 0:
            self._stale_claim_task = asyncio.create_task(
                self._run_stale_claim_cleanup(), name="stale-claim-cleanup"
            )

        logger.info(
            f"WorkerPool started with {len(self._workers)} workers, "
            f"{len(self._schedules)} schedules"
        )

    async def _emit_server_started(self) -> None:
        """Emit ServerStarted event to trigger startup handlers."""
        if self._container is None:
            return

        async with self._container(scope=Scope.UOW, context={Identity: System()}) as scope:
            outbox = await scope.get(Outbox)
            await outbox.append(ServerStarted(id=EventId(uuid4())))
        logger.info("ServerStarted event emitted")

    async def stop(self, timeout: float = 30.0) -> None:
        """Stop all workers gracefully.

        Args:
            timeout: Maximum time to wait for workers to stop.
        """
        self._shutdown = True

        # Signal all workers to stop
        for worker in self._workers:
            worker.stop()

        # Stop stale claim cleanup task
        if self._stale_claim_task and not self._stale_claim_task.done():
            self._stale_claim_task.cancel()
            try:
                await self._stale_claim_task
            except asyncio.CancelledError:
                pass

        # Wait for workers to finish with timeout
        tasks = [w._task for w in self._workers if w._task and not w._task.done()]
        if tasks:
            done, pending = await asyncio.wait(tasks, timeout=timeout)
            for task in pending:
                task.cancel()

        # Stop scheduler
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

            # Reset failure counter on success
            self._schedule_failures.pop(config.id, None)
            logger.debug(f"Ran schedule {config.id}")

        except (asyncio.CancelledError, SystemExit, KeyboardInterrupt):
            # Let control exceptions propagate for graceful shutdown
            raise
        except Exception as e:
            # Track consecutive failures
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
        """Periodically reset stale claims."""
        while not self._shutdown:
            try:
                await asyncio.sleep(self._stale_claim_interval)

                if self._shutdown or self._container is None:
                    break

                # Get max claim_timeout from all workers
                if self._workers:
                    max_timeout = max(w.config.claim_timeout for w in self._workers)

                    # Use a scoped outbox for cleanup
                    async with self._container(
                        scope=Scope.UOW, context={Identity: System()}
                    ) as scope:
                        outbox = await scope.get(Outbox)
                        count = await outbox.reset_stale_claims(max_timeout)
                        if count > 0:
                            logger.info(f"Reset {count} stale claims")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Stale claim cleanup failed: {e}")
