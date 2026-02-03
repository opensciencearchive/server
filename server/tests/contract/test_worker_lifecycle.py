"""Contract tests for WorkerPool lifecycle in FastAPI lifespan.

Tests for Phase 7: Migration.
"""

import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.handler.keyword_index_handler import KeywordIndexHandler
from osa.domain.index.handler.vector_index_handler import VectorIndexHandler
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.event import ClaimResult, EventId
from osa.domain.shared.model.srn import Domain, LocalId, RecordSRN, RecordVersion
from osa.infrastructure.event.worker import WorkerPool


class FakeBackend:
    """Fake storage backend for testing."""

    def __init__(self, name: str):
        self._name = name
        self.ingested: list[tuple[str, dict]] = []

    @property
    def name(self) -> str:
        return self._name

    async def ingest_batch(self, records: list[tuple[str, dict]]) -> None:
        self.ingested.extend(records)


def make_mock_container(handler_type: type, handler_instance: Any):
    """Create a mock DI container that provides scoped dependencies.

    Creates a container mock that returns scoped Outbox and AsyncSession
    when called as an async context manager with scope parameter.
    """
    from osa.domain.shared.outbox import Outbox

    outbox = AsyncMock()
    outbox.claim.return_value = ClaimResult(events=[], claimed_at=datetime.now(UTC))
    outbox.reset_stale_claims.return_value = 0

    session = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    async def get_dependency(cls: type) -> Any:
        """Return the appropriate dependency based on the requested class."""
        if cls == Outbox:
            return outbox
        if cls == handler_type:
            return handler_instance
        return session

    # Create scope that returns dependencies
    scope = AsyncMock()
    scope.get = AsyncMock(side_effect=get_dependency)

    # Create async context manager
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=scope)
    context.__aexit__ = AsyncMock(return_value=None)

    # Container callable returns the context manager
    container = MagicMock()
    container.return_value = context

    return container, outbox, session


class TestWorkerPoolLifecycle:
    """Tests for WorkerPool lifecycle management."""

    @pytest.mark.asyncio
    async def test_worker_pool_starts_and_stops_cleanly(self):
        """WorkerPool should start and stop without errors."""
        vector_backend = FakeBackend("vector")
        keyword_backend = FakeBackend("keyword")
        registry = IndexRegistry({"vector": vector_backend, "keyword": keyword_backend})

        # Create handler instance
        vector_handler = VectorIndexHandler(indexes=registry)

        container, outbox, session = make_mock_container(VectorIndexHandler, vector_handler)

        pool = WorkerPool(container=container, stale_claim_interval=60.0)
        pool.register(VectorIndexHandler)
        pool.register(KeywordIndexHandler)

        # Start
        await pool.start()
        await asyncio.sleep(0.05)

        # Verify workers are running
        assert len(pool.workers) == 2
        for worker in pool.workers:
            assert worker._task is not None
            assert not worker._task.done()

        # Stop
        await pool.stop()

        # Verify workers are stopped
        for worker in pool.workers:
            assert worker._shutdown is True

    @pytest.mark.asyncio
    async def test_worker_pool_as_context_manager(self):
        """WorkerPool should work as async context manager."""
        vector_backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": vector_backend})
        vector_handler = VectorIndexHandler(indexes=registry)

        container, outbox, session = make_mock_container(VectorIndexHandler, vector_handler)

        pool = WorkerPool(container=container, stale_claim_interval=60.0)
        pool.register(VectorIndexHandler)

        async with pool:
            # Workers should be running
            assert pool.workers[0]._task is not None
            await asyncio.sleep(0.02)

        # After exit, workers should be stopped
        assert pool.workers[0]._shutdown is True


class TestIndexHandlers:
    """Tests for concrete index handlers."""

    @pytest.mark.asyncio
    async def test_vector_handler_processes_batch(self):
        """VectorIndexHandler should process IndexRecord events in batches."""
        backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": backend})
        handler = VectorIndexHandler(indexes=registry)

        # Create test events
        events = [
            IndexRecord(
                id=EventId(uuid4()),
                backend_name="vector",
                record_srn=RecordSRN(
                    domain=Domain("test.example.com"),
                    id=LocalId(f"rec-{i}"),
                    version=RecordVersion(1),
                ),
                metadata={"title": f"Record {i}"},
            )
            for i in range(5)
        ]

        # Process
        await handler.handle_batch(events)

        # Verify backend received all records
        assert len(backend.ingested) == 5

    @pytest.mark.asyncio
    async def test_keyword_handler_processes_individually(self):
        """KeywordIndexHandler should process IndexRecord events one at a time."""
        backend = FakeBackend("keyword")
        registry = IndexRegistry({"keyword": backend})
        handler = KeywordIndexHandler(indexes=registry)

        # batch_size should be 1
        assert KeywordIndexHandler.__batch_size__ == 1

        # Create test event
        event = IndexRecord(
            id=EventId(uuid4()),
            backend_name="keyword",
            record_srn=RecordSRN(
                domain=Domain("test.example.com"),
                id=LocalId("rec-1"),
                version=RecordVersion(1),
            ),
            metadata={"title": "Record 1"},
        )

        # Process
        await handler.handle(event)

        # Verify
        assert len(backend.ingested) == 1

    @pytest.mark.asyncio
    async def test_handler_raises_on_backend_failure(self):
        """Handlers should raise when backend fails (Worker handles retry)."""
        # Backend that fails
        failing_backend = AsyncMock()
        failing_backend.name = "vector"
        failing_backend.ingest_batch = AsyncMock(side_effect=Exception("Backend error"))

        registry = IndexRegistry({"vector": failing_backend})
        handler = VectorIndexHandler(indexes=registry)

        event = IndexRecord(
            id=EventId(uuid4()),
            backend_name="vector",
            record_srn=RecordSRN(
                domain=Domain("test.example.com"),
                id=LocalId("rec-1"),
                version=RecordVersion(1),
            ),
            metadata={"title": "Record 1"},
        )

        # Process - should raise (Worker handles retry)
        with pytest.raises(Exception, match="Backend error"):
            await handler.handle_batch([event])
