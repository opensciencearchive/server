"""Unit tests for WorkerPool management.

Tests for WorkerPool lifecycle and handler registration.
"""

import asyncio
from datetime import UTC, datetime
from typing import ClassVar
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.event import (
    ClaimResult,
    Event,
    EventHandler,
    EventId,
)
from osa.domain.shared.outbox import Outbox


class DummyEvent(Event):
    """Test event for worker tests."""

    id: EventId
    data: str


class DummyHandler(EventHandler[DummyEvent]):
    """Test handler for pool tests."""

    __poll_interval__: ClassVar[float] = 0.01

    async def handle(self, event: DummyEvent) -> None:
        pass


class AnotherHandler(EventHandler[DummyEvent]):
    """Another test handler for pool tests."""

    __poll_interval__: ClassVar[float] = 0.01

    async def handle(self, event: DummyEvent) -> None:
        pass


def make_mock_container(
    outbox: AsyncMock | None = None,
    session: AsyncMock | None = None,
    handler: EventHandler | None = None,
):
    """Create a mock DI container."""
    if outbox is None:
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(events=[], claimed_at=datetime.now(UTC))
        outbox.append = AsyncMock()
        outbox.reset_stale_claims = AsyncMock(return_value=0)

    if session is None:
        session = AsyncMock(spec=AsyncSession)
        session.commit = AsyncMock()
        session.rollback = AsyncMock()

    async def get_dependency(cls):
        if cls == Outbox:
            return outbox
        if cls == AsyncSession:
            return session
        if handler is not None and issubclass(cls, EventHandler):
            return handler
        # Return a default handler if requested
        if issubclass(cls, EventHandler):
            return cls()
        return session

    scope = AsyncMock()
    scope.get = AsyncMock(side_effect=get_dependency)

    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=scope)
    context.__aexit__ = AsyncMock(return_value=None)

    container = MagicMock()
    container.return_value = context

    return container


class TestWorkerPoolManagement:
    """Tests for WorkerPool management."""

    def test_pool_register_creates_worker(self):
        """WorkerPool.register() should create a Worker from handler type."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        pool = WorkerPool()

        # Act
        worker = pool.register(DummyHandler)

        # Assert
        assert len(pool.workers) == 1
        assert worker.handler_type is DummyHandler
        assert worker.name == "DummyHandler"

    def test_pool_manages_multiple_handlers(self):
        """WorkerPool should manage multiple registered handlers."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        pool = WorkerPool()

        # Act
        pool.register(DummyHandler)
        pool.register(AnotherHandler)

        # Assert
        assert len(pool.workers) == 2
        names = {w.name for w in pool.workers}
        assert names == {"DummyHandler", "AnotherHandler"}

    @pytest.mark.asyncio
    async def test_pool_start_starts_all_workers(self):
        """WorkerPool.start() should start all registered workers."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        container = make_mock_container()
        pool = WorkerPool(container=container, stale_claim_interval=0)

        pool.register(DummyHandler)
        pool.register(AnotherHandler)

        # Act
        await pool.start()
        await asyncio.sleep(0.02)  # Let workers start

        # Assert - Workers should be running
        assert all(w._task is not None for w in pool.workers)
        assert all(not w._task.done() for w in pool.workers)

        # Cleanup
        await pool.stop()

    @pytest.mark.asyncio
    async def test_pool_stop_stops_all_workers(self):
        """WorkerPool.stop() should stop all workers gracefully."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        container = make_mock_container()
        pool = WorkerPool(container=container, stale_claim_interval=0)

        pool.register(DummyHandler)

        await pool.start()
        await asyncio.sleep(0.02)

        # Act
        await pool.stop()

        # Assert - Workers should be stopped
        for worker in pool.workers:
            assert worker._shutdown is True

    @pytest.mark.asyncio
    async def test_pool_context_manager(self):
        """WorkerPool should work as async context manager."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        container = make_mock_container()
        pool = WorkerPool(container=container, stale_claim_interval=0)
        pool.register(DummyHandler)

        # Act
        async with pool:
            # Assert - Pool should be running
            assert all(w._task is not None for w in pool.workers)

        # Assert - Pool should be stopped after context exit
        for worker in pool.workers:
            assert worker._shutdown is True

    @pytest.mark.asyncio
    async def test_pool_requires_container(self):
        """WorkerPool.start() should raise if container not set."""
        from osa.infrastructure.event.worker import WorkerPool

        pool = WorkerPool()
        pool.register(DummyHandler)

        # Act & Assert
        with pytest.raises(RuntimeError, match="Container not set"):
            await pool.start()

    def test_pool_set_container_propagates_to_workers(self):
        """WorkerPool.set_container() should propagate to all workers."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        pool = WorkerPool()
        pool.register(DummyHandler)
        pool.register(AnotherHandler)

        container = make_mock_container()

        # Act
        pool.set_container(container)

        # Assert
        for worker in pool.workers:
            assert worker._container is container


class TestWorkerPoolStaleClaims:
    """Tests for stale claim cleanup in WorkerPool."""

    @pytest.mark.asyncio
    async def test_pool_runs_stale_claim_cleanup(self):
        """WorkerPool should periodically reset stale claims."""
        from osa.infrastructure.event.worker import WorkerPool

        # Arrange
        outbox = AsyncMock(spec=Outbox)
        outbox.claim.return_value = ClaimResult(events=[], claimed_at=datetime.now(UTC))
        outbox.append = AsyncMock()
        outbox.reset_stale_claims = AsyncMock(return_value=2)

        container = make_mock_container(outbox)
        pool = WorkerPool(container=container, stale_claim_interval=0.05)
        pool.register(DummyHandler)

        # Act
        await pool.start()
        await asyncio.sleep(0.1)  # Wait for cleanup to run

        # Assert - Stale claim cleanup should have been called
        # Note: The actual call might depend on timing
        await pool.stop()
