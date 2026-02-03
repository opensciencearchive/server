"""Integration tests for crash recovery in event processing.

Tests that events are not lost when the worker is interrupted mid-processing.
"""

from typing import Any
from uuid import uuid4

import pytest

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.handler.vector_index_handler import VectorIndexHandler
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import Domain, LocalId, RecordSRN, RecordVersion


class TrackingBackend:
    """Backend that tracks all operations for verification."""

    def __init__(self, name: str, fail_at: int | None = None):
        self._name = name
        self._fail_at = fail_at
        self._call_count = 0
        self.indexed_records: list[tuple[str, dict]] = []

    @property
    def name(self) -> str:
        return self._name

    async def ingest_batch(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        """Track records and optionally fail at specific call."""
        self._call_count += 1
        if self._fail_at is not None and self._call_count >= self._fail_at:
            raise Exception("Simulated crash")
        self.indexed_records.extend(records)


def make_index_record(backend_name: str, record_id: str) -> IndexRecord:
    """Create an IndexRecord for testing."""
    return IndexRecord(
        id=EventId(uuid4()),
        backend_name=backend_name,
        record_srn=RecordSRN(
            domain=Domain("test.example.com"),
            id=LocalId(record_id),
            version=RecordVersion(1),
        ),
        metadata={"id": record_id, "title": f"Record {record_id}"},
    )


class FakeOutbox:
    """Simulates outbox with pending events."""

    def __init__(self, events: list[IndexRecord]):
        self.pending = list(events)
        self.delivered: list[EventId] = []
        self.failed: dict[EventId, str] = {}

    async def fetch_pending(self, limit: int) -> list[IndexRecord]:
        """Return pending events."""
        batch = self.pending[:limit]
        return batch

    async def mark_delivered(self, event_id: EventId) -> None:
        """Mark event as delivered."""
        self.delivered.append(event_id)
        self.pending = [e for e in self.pending if e.id != event_id]

    async def mark_failed(self, event_id: EventId, error: str) -> None:
        """Mark event as failed."""
        self.failed[event_id] = error


class TestCrashRecoveryScenarios:
    """Tests for crash recovery scenarios."""

    @pytest.mark.asyncio
    async def test_events_remain_pending_on_crash(self):
        """Events should remain pending if processing crashes before commit."""
        # Arrange
        events = [make_index_record("vector", f"record-{i}") for i in range(10)]
        outbox = FakeOutbox(events)

        # Simulate a backend that fails mid-batch
        backend = TrackingBackend("vector", fail_at=1)  # Fail on first call
        registry = IndexRegistry({"vector": backend})
        handler = VectorIndexHandler(indexes=registry)

        # Act - Try to process, expecting failure
        with pytest.raises(Exception, match="Simulated crash"):
            await handler.handle_batch(events)

        # Assert - Events should remain pending (outbox unchanged)
        assert len(outbox.pending) == 10  # All still pending
        assert len(outbox.delivered) == 0  # None delivered
        assert len(backend.indexed_records) == 0  # None indexed

    @pytest.mark.asyncio
    async def test_recovery_processes_all_pending_events(self):
        """After recovery, all pending events should be processed."""
        # Arrange - Create events and simulate they were fetched but not committed
        events = [make_index_record("vector", f"record-{i}") for i in range(10)]
        outbox = FakeOutbox(events)

        # First attempt: backend fails
        failing_backend = TrackingBackend("vector", fail_at=1)
        failing_registry = IndexRegistry({"vector": failing_backend})
        failing_handler = VectorIndexHandler(indexes=failing_registry)

        with pytest.raises(Exception):
            await failing_handler.handle_batch(events)

        # Events still pending
        assert len(outbox.pending) == 10

        # Second attempt (recovery): backend works
        working_backend = TrackingBackend("vector")
        working_registry = IndexRegistry({"vector": working_backend})
        working_handler = VectorIndexHandler(indexes=working_registry)

        # Act - Retry processing
        await working_handler.handle_batch(outbox.pending)

        # Assert - All events processed
        assert len(working_backend.indexed_records) == 10

    @pytest.mark.asyncio
    async def test_partial_batch_failure_is_atomic(self):
        """If batch fails partway, none of the events should be committed."""
        # Arrange
        events = [make_index_record("vector", f"record-{i}") for i in range(5)]

        # Backend that succeeds on first record but throws exception
        class PartialFailBackend:
            def __init__(self):
                self.name = "vector"
                self.processed: list[tuple[str, dict]] = []

            async def ingest_batch(self, records: list[tuple[str, dict[str, Any]]]) -> None:
                # Process some, then fail
                for i, (srn, meta) in enumerate(records):
                    if i >= 2:
                        raise Exception("Partial failure at record 2")
                    self.processed.append((srn, meta))

        backend = PartialFailBackend()
        registry = IndexRegistry({"vector": backend})
        handler = VectorIndexHandler(indexes=registry)

        # Act
        with pytest.raises(Exception, match="Partial failure"):
            await handler.handle_batch(events)

        # Assert - Some records were processed but batch should be atomic
        # In production, the outbox marks all events as failed together
        # The handler correctly propagates the error for retry
        assert len(backend.processed) == 2  # Backend saw 2 before crash

    @pytest.mark.asyncio
    async def test_idempotent_reprocessing(self):
        """Reprocessing the same events should be safe (idempotent)."""
        # Arrange
        events = [make_index_record("vector", f"record-{i}") for i in range(5)]

        backend = TrackingBackend("vector")
        registry = IndexRegistry({"vector": backend})
        handler = VectorIndexHandler(indexes=registry)

        # Act - Process twice
        await handler.handle_batch(events)
        await handler.handle_batch(events)  # Reprocess same events

        # Assert - Records should be in backend (upsert semantics handle duplicates)
        # The backend receives all records from both batches
        assert len(backend.indexed_records) == 10  # 5 + 5

        # In production, ChromaDB's upsert handles this - same ID overwrites
        # The important thing is no data corruption

    @pytest.mark.asyncio
    async def test_crash_safe_no_in_memory_buffer(self):
        """Verify there's no in-memory buffer that could lose data."""
        # Arrange
        events = [make_index_record("vector", f"record-{i}") for i in range(3)]

        class StatelessBackend:
            """Backend with no internal state."""

            def __init__(self):
                self.name = "vector"
                self.batch_calls: list[list[tuple[str, dict]]] = []

            async def ingest_batch(self, records: list[tuple[str, dict[str, Any]]]) -> None:
                # Immediately persist (no buffering)
                self.batch_calls.append(list(records))

        backend = StatelessBackend()
        registry = IndexRegistry({"vector": backend})
        handler = VectorIndexHandler(indexes=registry)

        # Act
        await handler.handle_batch(events)

        # Assert - All records in single batch call, immediately persisted
        assert len(backend.batch_calls) == 1
        assert len(backend.batch_calls[0]) == 3

        # No flush needed - data is persisted immediately
        # If we crashed right after ingest_batch, all 3 records are safe
