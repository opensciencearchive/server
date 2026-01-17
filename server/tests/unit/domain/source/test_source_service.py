"""Unit tests for SourceService."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.config import Config
from osa.domain.shared.model.srn import Domain
from osa.domain.shared.outbox import Outbox
from osa.domain.source.model.registry import SourceRegistry
from osa.domain.source.service.source import SourceService
from osa.sdk.source.record import UpstreamRecord


class FakeSource:
    """Fake source for testing."""

    name = "fake-source"

    def __init__(self, records: list[UpstreamRecord]):
        self._records = records

    async def pull(self, since: datetime | None = None, limit: int | None = None):
        for record in self._records[: limit if limit else len(self._records)]:
            yield record


@pytest.fixture
def mock_outbox() -> Outbox:
    """Create a mock Outbox."""
    outbox = MagicMock(spec=Outbox)
    outbox.append = AsyncMock()
    return outbox


@pytest.fixture
def mock_config() -> Config:
    """Create a mock Config with server domain."""
    config = MagicMock(spec=Config)
    config.server = MagicMock()
    config.server.domain = "test.example.com"
    return config


@pytest.fixture
def sample_records() -> list[UpstreamRecord]:
    """Create sample upstream records for testing."""
    now = datetime.now(timezone.utc)
    return [
        UpstreamRecord(
            source_id="GSE001",
            source_type="geo",
            metadata={"title": "Test Record 1", "organism": "human"},
            fetched_at=now,
        ),
        UpstreamRecord(
            source_id="GSE002",
            source_type="geo",
            metadata={"title": "Test Record 2", "organism": "mouse"},
            fetched_at=now,
        ),
    ]


class TestSourceService:
    """Tests for SourceService."""

    @pytest.mark.asyncio
    async def test_run_source_emits_deposition_events(
        self,
        mock_outbox: Outbox,
        mock_config: Config,
        sample_records: list[UpstreamRecord],
    ):
        """Service should emit DepositionSubmittedEvent for each record."""
        # Arrange
        fake_source = FakeSource(sample_records)
        registry = SourceRegistry({"fake": fake_source})

        service = SourceService(
            sources=registry,
            outbox=mock_outbox,
            node_domain=Domain(mock_config.server.domain),
        )

        # Act
        result = await service.run_source(
            source_name="fake",
            since=None,
            limit=None,
        )

        # Assert
        assert result.record_count == 2
        assert result.source_name == "fake"
        # Two DepositionSubmittedEvent + one SourceRunCompleted
        assert mock_outbox.append.call_count == 3

    @pytest.mark.asyncio
    async def test_run_source_with_limit(
        self,
        mock_outbox: Outbox,
        mock_config: Config,
        sample_records: list[UpstreamRecord],
    ):
        """Service should respect limit parameter."""
        # Arrange
        fake_source = FakeSource(sample_records)
        registry = SourceRegistry({"fake": fake_source})

        service = SourceService(
            sources=registry,
            outbox=mock_outbox,
            node_domain=Domain(mock_config.server.domain),
        )

        # Act
        result = await service.run_source(
            source_name="fake",
            since=None,
            limit=1,
        )

        # Assert
        assert result.record_count == 1

    @pytest.mark.asyncio
    async def test_run_source_unknown_source_raises(
        self,
        mock_outbox: Outbox,
        mock_config: Config,
    ):
        """Service should raise error for unknown source."""
        # Arrange
        registry = SourceRegistry({})

        service = SourceService(
            sources=registry,
            outbox=mock_outbox,
            node_domain=Domain(mock_config.server.domain),
        )

        # Act & Assert
        with pytest.raises(ValueError, match="Unknown source"):
            await service.run_source(
                source_name="nonexistent",
                since=None,
                limit=None,
            )

    @pytest.mark.asyncio
    async def test_run_source_emits_completion_event(
        self,
        mock_outbox: Outbox,
        mock_config: Config,
        sample_records: list[UpstreamRecord],
    ):
        """Service should emit SourceRunCompleted event after pulling."""
        # Arrange
        fake_source = FakeSource(sample_records)
        registry = SourceRegistry({"fake": fake_source})

        service = SourceService(
            sources=registry,
            outbox=mock_outbox,
            node_domain=Domain(mock_config.server.domain),
        )

        # Act
        await service.run_source(
            source_name="fake",
            since=None,
            limit=None,
        )

        # Assert - last call should be the completion event
        from osa.domain.source.event.source_run_completed import SourceRunCompleted

        last_call = mock_outbox.append.call_args_list[-1]
        event = last_call[0][0]
        assert isinstance(event, SourceRunCompleted)
        assert event.record_count == 2
        assert event.source_name == "fake"
