"""Integration tests for batch event processing flow.

Tests the end-to-end flow from RecordPublished -> IndexRecord fan-out -> batch processing.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.handler.fanout_to_index_backends import FanOutToIndexBackends
from osa.domain.index.handler.vector_index_handler import VectorIndexHandler
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId, RecordSRN, RecordVersion


class FakeBackend:
    """Fake storage backend that tracks batch calls."""

    def __init__(self, name: str):
        self._name = name
        self.batch_calls: list[list[tuple[str, dict]]] = []
        self.total_records: int = 0

    @property
    def name(self) -> str:
        return self._name

    async def ingest_batch(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        """Track batch calls for verification."""
        self.batch_calls.append(list(records))
        self.total_records += len(records)


class FakeOutbox:
    """Fake outbox that collects emitted events."""

    def __init__(self):
        self.events: list[Any] = []

    async def append(self, event: Any, routing_key: str | None = None) -> None:
        self.events.append(event)


def make_record_published(
    record_id: str | None = None,
    metadata: dict | None = None,
) -> RecordPublished:
    """Create a RecordPublished event for testing."""
    return RecordPublished(
        id=EventId(uuid4()),
        record_srn=RecordSRN(
            domain=Domain("test.example.com"),
            id=LocalId(record_id or str(uuid4())),
            version=RecordVersion(1),
        ),
        deposition_srn=DepositionSRN(
            domain=Domain("test.example.com"),
            id=LocalId(str(uuid4())),
        ),
        metadata=metadata or {"title": "Test Record"},
    )


class TestBatchEventProcessingFlow:
    """Integration tests for the batch event processing flow."""

    @pytest.mark.asyncio
    async def test_fanout_creates_index_records_per_backend(self):
        """FanOutToIndexBackends should create one IndexRecord per backend."""
        # Arrange
        backends = {
            "vector": FakeBackend("vector"),
            "keyword": FakeBackend("keyword"),
        }
        registry = IndexRegistry(backends)
        outbox = FakeOutbox()

        fanout = FanOutToIndexBackends(indexes=registry, outbox=outbox)
        event = make_record_published()

        # Act
        await fanout.handle(event)

        # Assert
        assert len(outbox.events) == 2
        backend_names = {e.backend_name for e in outbox.events}
        assert backend_names == {"vector", "keyword"}

    @pytest.mark.asyncio
    async def test_handler_processes_batch(self):
        """VectorIndexHandler should process events in batches."""
        # Arrange
        vector_backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": vector_backend})
        handler = VectorIndexHandler(indexes=registry)

        # Create events for vector backend
        events = [
            IndexRecord(
                id=EventId(uuid4()),
                backend_name="vector",
                record_srn=RecordSRN(
                    domain=Domain("test.example.com"),
                    id=LocalId(str(uuid4())),
                    version=RecordVersion(1),
                ),
                metadata={"id": i},
            )
            for i in range(5)
        ]

        # Act
        await handler.handle_batch(events)

        # Assert - vector backend received 5 records in single batch call
        assert len(vector_backend.batch_calls) == 1
        assert len(vector_backend.batch_calls[0]) == 5
        assert vector_backend.total_records == 5

    @pytest.mark.asyncio
    async def test_end_to_end_fanout_to_batch(self):
        """End-to-end: RecordPublished -> FanOut -> BatchProcess."""
        # Arrange
        vector_backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": vector_backend})
        outbox = FakeOutbox()

        fanout = FanOutToIndexBackends(indexes=registry, outbox=outbox)
        handler = VectorIndexHandler(indexes=registry)

        # Create multiple RecordPublished events
        num_records = 10
        published_events = [make_record_published() for _ in range(num_records)]

        # Act - Step 1: FanOut each RecordPublished
        for event in published_events:
            await fanout.handle(event)

        # Verify IndexRecord events were created
        assert len(outbox.events) == num_records
        assert all(isinstance(e, IndexRecord) for e in outbox.events)
        assert all(e.backend_name == "vector" for e in outbox.events)

        # Act - Step 2: Batch process all IndexRecord events
        await handler.handle_batch(outbox.events)

        # Assert - All records indexed in single batch call
        assert len(vector_backend.batch_calls) == 1
        assert vector_backend.total_records == num_records

    @pytest.mark.asyncio
    async def test_batch_efficiency_large_batch(self):
        """Verify batch processing is efficient with large batches."""
        # Arrange
        backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": backend})
        handler = VectorIndexHandler(indexes=registry)

        # Create 1000+ events
        num_events = 1000
        events = [
            IndexRecord(
                id=EventId(uuid4()),
                backend_name="vector",
                record_srn=RecordSRN(
                    domain=Domain("test.example.com"),
                    id=LocalId(str(uuid4())),
                    version=RecordVersion(1),
                ),
                metadata={"index": i, "title": f"Record {i}"},
            )
            for i in range(num_events)
        ]

        # Act
        await handler.handle_batch(events)

        # Assert - All records in single batch call (not 1000 individual calls)
        assert len(backend.batch_calls) == 1, "Should use single batch call, not individual calls"
        assert backend.total_records == num_events
        assert len(backend.batch_calls[0]) == num_events

    @pytest.mark.asyncio
    async def test_failure_propagates_from_handler(self):
        """Verify failures propagate from handler (Worker handles retry)."""
        # Arrange - VectorIndexHandler looks up "vector" backend specifically
        vector_backend = MagicMock()
        vector_backend.name = "vector"
        vector_backend.ingest_batch = AsyncMock(side_effect=Exception("Backend failure"))

        registry = IndexRegistry({"vector": vector_backend})
        handler = VectorIndexHandler(indexes=registry)

        events = [
            IndexRecord(
                id=EventId(uuid4()),
                backend_name="vector",
                record_srn=RecordSRN(
                    domain=Domain("test.example.com"),
                    id=LocalId(str(uuid4())),
                    version=RecordVersion(1),
                ),
                metadata={"id": i},
            )
            for i in range(2)
        ]

        # Process events - should raise (Worker handles retry)
        with pytest.raises(Exception, match="Backend failure"):
            await handler.handle_batch(events)
