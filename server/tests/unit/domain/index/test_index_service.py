"""Unit tests for IndexService."""

from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.index.service.index import IndexService
from osa.domain.shared.model.srn import Domain, LocalId, RecordSRN, RecordVersion


class FakeBackend:
    """Fake storage backend for testing."""

    def __init__(self):
        self.ingested: list[tuple[str, dict]] = []
        self.ingest = AsyncMock(side_effect=self._ingest)

    async def _ingest(self, record_id: str, metadata: dict[str, Any]) -> None:
        self.ingested.append((record_id, metadata))


class FailingBackend:
    """Backend that always fails for testing error handling."""

    def __init__(self):
        self.ingest = AsyncMock(side_effect=Exception("Backend failure"))


@pytest.fixture
def sample_record_srn() -> RecordSRN:
    """Create a sample record SRN."""
    return RecordSRN(
        domain=Domain("test.example.com"),
        id=LocalId(str(uuid4())),
        version=RecordVersion(1),
    )


@pytest.fixture
def sample_metadata() -> dict:
    """Create sample metadata for testing."""
    return {
        "title": "Test Record",
        "organism": "human",
        "platform": "GPL570",
    }


class TestIndexService:
    """Tests for IndexService."""

    @pytest.mark.asyncio
    async def test_index_record_indexes_to_all_backends(
        self,
        sample_record_srn: RecordSRN,
        sample_metadata: dict,
    ):
        """Service should index record to all configured backends."""
        # Arrange
        backend1 = FakeBackend()
        backend2 = FakeBackend()
        registry = IndexRegistry({"backend1": backend1, "backend2": backend2})

        service = IndexService(indexes=registry)

        # Act
        await service.index_record(
            record_srn=sample_record_srn,
            metadata=sample_metadata,
        )

        # Assert
        assert len(backend1.ingested) == 1
        assert len(backend2.ingested) == 1
        assert backend1.ingested[0][0] == str(sample_record_srn)
        assert backend1.ingested[0][1] == sample_metadata

    @pytest.mark.asyncio
    async def test_index_record_handles_backend_failure(
        self,
        sample_record_srn: RecordSRN,
        sample_metadata: dict,
    ):
        """Service should continue indexing to other backends if one fails."""
        # Arrange
        failing_backend = FailingBackend()
        working_backend = FakeBackend()
        registry = IndexRegistry(
            {
                "failing": failing_backend,
                "working": working_backend,
            }
        )

        service = IndexService(indexes=registry)

        # Act - should not raise, just log error
        await service.index_record(
            record_srn=sample_record_srn,
            metadata=sample_metadata,
        )

        # Assert - working backend should still have received the record
        assert len(working_backend.ingested) == 1
        assert working_backend.ingested[0][0] == str(sample_record_srn)

    @pytest.mark.asyncio
    async def test_index_record_empty_registry(
        self,
        sample_record_srn: RecordSRN,
        sample_metadata: dict,
    ):
        """Service should handle empty registry gracefully."""
        # Arrange
        registry = IndexRegistry({})
        service = IndexService(indexes=registry)

        # Act - should not raise
        await service.index_record(
            record_srn=sample_record_srn,
            metadata=sample_metadata,
        )

        # Assert - no exception means success
