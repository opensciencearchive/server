"""Contract tests for WorkerPool lifecycle in FastAPI lifespan.

Tests for Phase 7: Migration.
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.worker import KeywordIndexWorker, VectorIndexWorker
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


def make_mock_container():
    """Create a mock DI container that provides scoped Outbox and Session.

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

    async def get_dependency(cls):
        """Return the appropriate dependency based on the requested class."""
        if cls == Outbox:
            return outbox
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
        container, outbox, session = make_mock_container()

        vector_backend = FakeBackend("vector")
        keyword_backend = FakeBackend("keyword")

        pool = WorkerPool(container=container, stale_claim_interval=60.0)
        pool.add_worker(VectorIndexWorker(vector_backend))
        pool.add_worker(KeywordIndexWorker(keyword_backend))

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
        container, outbox, session = make_mock_container()

        pool = WorkerPool(container=container, stale_claim_interval=60.0)
        pool.add_worker(VectorIndexWorker(FakeBackend("vector"), batch_size=10))

        async with pool:
            # Workers should be running
            assert pool.workers[0]._task is not None
            await asyncio.sleep(0.02)

        # After exit, workers should be stopped
        assert pool.workers[0]._shutdown is True


class TestIndexWorkers:
    """Tests for concrete index workers."""

    @pytest.mark.asyncio
    async def test_vector_worker_processes_batch(self):
        """VectorIndexWorker should process IndexRecord events in batches."""
        outbox = AsyncMock()
        session = AsyncMock()
        backend = FakeBackend("vector")

        worker = VectorIndexWorker(backend, batch_size=10)

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
                routing_key="vector",
            )
            for i in range(5)
        ]

        # Process
        await worker.process_events(events, outbox, session)

        # Verify backend received all records
        assert len(backend.ingested) == 5

        # Verify all events marked as delivered
        assert outbox.mark_delivered.call_count == 5

    @pytest.mark.asyncio
    async def test_keyword_worker_processes_individually(self):
        """KeywordIndexWorker should process IndexRecord events one at a time."""
        outbox = AsyncMock()
        session = AsyncMock()
        backend = FakeBackend("keyword")

        worker = KeywordIndexWorker(backend)

        # batch_size should be 1
        assert worker.config.batch_size == 1

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
            routing_key="keyword",
        )

        # Process
        await worker.process_events([event], outbox, session)

        # Verify
        assert len(backend.ingested) == 1
        outbox.mark_delivered.assert_called_once()

    @pytest.mark.asyncio
    async def test_worker_handles_backend_failure(self):
        """Workers should mark events as failed when backend fails."""
        outbox = AsyncMock()
        session = AsyncMock()

        # Backend that fails
        failing_backend = AsyncMock()
        failing_backend.ingest_batch = AsyncMock(side_effect=Exception("Backend error"))

        worker = VectorIndexWorker(failing_backend, batch_size=10)

        event = IndexRecord(
            id=EventId(uuid4()),
            backend_name="vector",
            record_srn=RecordSRN(
                domain=Domain("test.example.com"),
                id=LocalId("rec-1"),
                version=RecordVersion(1),
            ),
            metadata={"title": "Record 1"},
            routing_key="vector",
        )

        # Process - should not raise
        await worker.process_events([event], outbox, session)

        # Event should be marked as failed with retry
        outbox.mark_failed_with_retry.assert_called_once()
        call_args = outbox.mark_failed_with_retry.call_args
        assert call_args[0][0] == event.id
        assert "Backend error" in call_args[0][1]
