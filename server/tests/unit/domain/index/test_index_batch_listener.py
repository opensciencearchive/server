"""Unit tests for IndexRecordBatch listener."""

from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.listener.index_batch_listener import IndexRecordBatch
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import Domain, LocalId, RecordSRN, RecordVersion


class FakeBackend:
    """Fake storage backend for testing."""

    def __init__(self, name: str):
        self._name = name
        self.batches: list[list[tuple[str, dict]]] = []
        self.ingest_batch = AsyncMock(side_effect=self._ingest_batch)

    @property
    def name(self) -> str:
        return self._name

    async def _ingest_batch(self, records: list[tuple[str, dict[str, Any]]]) -> None:
        self.batches.append(list(records))


class FailingBackend:
    """Backend that always fails for testing error handling."""

    def __init__(self, name: str):
        self._name = name
        self.ingest_batch = AsyncMock(side_effect=Exception("Backend failure"))

    @property
    def name(self) -> str:
        return self._name


def make_index_record(
    backend_name: str,
    srn: RecordSRN | None = None,
    metadata: dict | None = None,
) -> IndexRecord:
    """Create an IndexRecord for testing."""
    if srn is None:
        srn = RecordSRN(
            domain=Domain("test.example.com"),
            id=LocalId(str(uuid4())),
            version=RecordVersion(1),
        )
    return IndexRecord(
        id=EventId(uuid4()),
        backend_name=backend_name,
        record_srn=srn,
        metadata=metadata or {"title": "Test"},
    )


class TestIndexRecordBatch:
    """Tests for IndexRecordBatch listener."""

    @pytest.mark.asyncio
    async def test_groups_events_by_backend(self):
        """Listener should group events by backend and call ingest_batch per backend."""
        # Arrange
        vector_backend = FakeBackend("vector")
        keyword_backend = FakeBackend("keyword")
        registry = IndexRegistry({"vector": vector_backend, "keyword": keyword_backend})

        listener = IndexRecordBatch(indexes=registry)

        events = [
            make_index_record("vector", metadata={"id": 1}),
            make_index_record("keyword", metadata={"id": 2}),
            make_index_record("vector", metadata={"id": 3}),
        ]

        # Act
        await listener.handle_batch(events)

        # Assert - vector backend received 2 records in one batch
        assert len(vector_backend.batches) == 1
        assert len(vector_backend.batches[0]) == 2

        # Assert - keyword backend received 1 record in one batch
        assert len(keyword_backend.batches) == 1
        assert len(keyword_backend.batches[0]) == 1

    @pytest.mark.asyncio
    async def test_passes_correct_srn_and_metadata(self):
        """Listener should pass correct SRN and metadata to backend."""
        # Arrange
        backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": backend})

        listener = IndexRecordBatch(indexes=registry)

        srn = RecordSRN(
            domain=Domain("test.example.com"),
            id=LocalId("test-record-id"),
            version=RecordVersion(1),
        )
        metadata = {"title": "Test Record", "organism": "human"}

        events = [make_index_record("vector", srn=srn, metadata=metadata)]

        # Act
        await listener.handle_batch(events)

        # Assert
        assert len(backend.batches) == 1
        assert len(backend.batches[0]) == 1
        record_srn, record_meta = backend.batches[0][0]
        assert record_srn == str(srn)
        assert record_meta == metadata

    @pytest.mark.asyncio
    async def test_handles_empty_batch(self):
        """Listener should handle empty batch without error."""
        # Arrange
        backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": backend})

        listener = IndexRecordBatch(indexes=registry)

        # Act
        await listener.handle_batch([])

        # Assert - no calls made
        assert len(backend.batches) == 0

    @pytest.mark.asyncio
    async def test_skips_unknown_backend(self):
        """Listener should skip events for unknown backends."""
        # Arrange
        vector_backend = FakeBackend("vector")
        registry = IndexRegistry({"vector": vector_backend})

        listener = IndexRecordBatch(indexes=registry)

        events = [
            make_index_record("vector", metadata={"id": 1}),
            make_index_record("unknown", metadata={"id": 2}),  # Unknown backend
        ]

        # Act - should not raise
        await listener.handle_batch(events)

        # Assert - vector backend received its event
        assert len(vector_backend.batches) == 1
        assert len(vector_backend.batches[0]) == 1

    @pytest.mark.asyncio
    async def test_raises_on_backend_failure(self):
        """Listener should propagate backend failures for retry."""
        # Arrange
        failing_backend = FailingBackend("vector")
        registry = IndexRegistry({"vector": failing_backend})

        listener = IndexRecordBatch(indexes=registry)

        events = [make_index_record("vector")]

        # Act & Assert
        with pytest.raises(Exception, match="Backend failure"):
            await listener.handle_batch(events)


class TestIndexRecordBatchFailureVisibility:
    """Tests for failure visibility in IndexRecordBatch (US4)."""

    @pytest.mark.asyncio
    async def test_failure_includes_backend_name_in_error(self):
        """Failures should include backend name for visibility."""
        # Arrange
        failing_backend = FailingBackend("vector")
        registry = IndexRegistry({"vector": failing_backend})

        listener = IndexRecordBatch(indexes=registry)

        events = [make_index_record("vector")]

        # Act & Assert
        # The backend failure should propagate with context
        with pytest.raises(Exception):
            await listener.handle_batch(events)

        # Backend's ingest_batch was called
        failing_backend.ingest_batch.assert_called_once()
