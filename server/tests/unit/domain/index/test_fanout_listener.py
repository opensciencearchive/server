"""Unit tests for FanOutToIndexBackends handler."""

from typing import Any
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.index.event.index_record import IndexRecord
from osa.domain.index.handler.fanout_to_index_backends import FanOutToIndexBackends
from osa.domain.index.model.registry import IndexRegistry
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId, RecordSRN, RecordVersion


class FakeBackend:
    """Fake storage backend for testing."""

    def __init__(self, name: str):
        self._name = name

    @property
    def name(self) -> str:
        return self._name


class FakeOutbox:
    """Fake outbox for testing event emission."""

    def __init__(self):
        self.events: list[Any] = []
        self.append = AsyncMock(side_effect=self._append)

    async def _append(self, event: Any, routing_key: str | None = None) -> None:
        self.events.append(event)


@pytest.fixture
def sample_record_srn() -> RecordSRN:
    """Create a sample record SRN."""
    return RecordSRN(
        domain=Domain("test.example.com"),
        id=LocalId(str(uuid4())),
        version=RecordVersion(1),
    )


@pytest.fixture
def sample_deposition_srn() -> DepositionSRN:
    """Create a sample deposition SRN."""
    return DepositionSRN(
        domain=Domain("test.example.com"),
        id=LocalId(str(uuid4())),
    )


@pytest.fixture
def sample_metadata() -> dict:
    """Create sample metadata for testing."""
    return {
        "title": "Test Record",
        "organism": "human",
        "platform": "GPL570",
    }


class TestFanOutToIndexBackends:
    """Tests for FanOutToIndexBackends handler."""

    @pytest.mark.asyncio
    async def test_creates_index_record_per_backend(
        self,
        sample_record_srn: RecordSRN,
        sample_deposition_srn: DepositionSRN,
        sample_metadata: dict,
    ):
        """Handler should create one IndexRecord event per registered backend."""
        # Arrange
        backend1 = FakeBackend("vector")
        backend2 = FakeBackend("keyword")
        registry = IndexRegistry({"vector": backend1, "keyword": backend2})
        outbox = FakeOutbox()

        handler = FanOutToIndexBackends(indexes=registry, outbox=outbox)

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=sample_record_srn,
            deposition_srn=sample_deposition_srn,
            metadata=sample_metadata,
        )

        # Act
        await handler.handle(event)

        # Assert
        assert len(outbox.events) == 2
        backend_names = {e.backend_name for e in outbox.events}
        assert backend_names == {"vector", "keyword"}

        for index_event in outbox.events:
            assert isinstance(index_event, IndexRecord)
            assert index_event.record_srn == sample_record_srn
            assert index_event.metadata == sample_metadata

    @pytest.mark.asyncio
    async def test_creates_unique_event_ids(
        self,
        sample_record_srn: RecordSRN,
        sample_deposition_srn: DepositionSRN,
        sample_metadata: dict,
    ):
        """Each IndexRecord should have a unique event ID."""
        # Arrange
        registry = IndexRegistry(
            {
                "backend1": FakeBackend("backend1"),
                "backend2": FakeBackend("backend2"),
            }
        )
        outbox = FakeOutbox()

        handler = FanOutToIndexBackends(indexes=registry, outbox=outbox)

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=sample_record_srn,
            deposition_srn=sample_deposition_srn,
            metadata=sample_metadata,
        )

        # Act
        await handler.handle(event)

        # Assert
        event_ids = [e.id for e in outbox.events]
        assert len(event_ids) == len(set(event_ids)), "Event IDs should be unique"

    @pytest.mark.asyncio
    async def test_empty_registry_creates_no_events(
        self,
        sample_record_srn: RecordSRN,
        sample_deposition_srn: DepositionSRN,
        sample_metadata: dict,
    ):
        """No IndexRecord events should be created if registry is empty."""
        # Arrange
        registry = IndexRegistry({})
        outbox = FakeOutbox()

        handler = FanOutToIndexBackends(indexes=registry, outbox=outbox)

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=sample_record_srn,
            deposition_srn=sample_deposition_srn,
            metadata=sample_metadata,
        )

        # Act
        await handler.handle(event)

        # Assert
        assert len(outbox.events) == 0
