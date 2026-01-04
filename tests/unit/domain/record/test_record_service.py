"""Unit tests for RecordService."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.service.record import RecordService
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox


@pytest.fixture
def mock_record_repo() -> RecordRepository:
    """Create a mock RecordRepository."""
    repo = MagicMock(spec=RecordRepository)
    repo.save = AsyncMock()
    return repo


@pytest.fixture
def mock_outbox() -> Outbox:
    """Create a mock Outbox."""
    outbox = MagicMock(spec=Outbox)
    outbox.append = AsyncMock()
    return outbox


@pytest.fixture
def node_domain() -> Domain:
    """Create test node domain."""
    return Domain("test.example.com")


@pytest.fixture
def sample_deposition_srn(node_domain: Domain) -> DepositionSRN:
    """Create a sample deposition SRN."""
    return DepositionSRN(
        domain=node_domain,
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


class TestRecordService:
    """Tests for RecordService."""

    @pytest.mark.asyncio
    async def test_publish_record_creates_record(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_deposition_srn: DepositionSRN,
        sample_metadata: dict,
    ):
        """Service should create and persist a Record."""
        # Arrange
        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
        )

        # Act
        record = await service.publish_record(
            deposition_srn=sample_deposition_srn,
            metadata=sample_metadata,
        )

        # Assert
        assert record is not None
        assert record.deposition_srn == sample_deposition_srn
        assert record.metadata == sample_metadata
        mock_record_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_record_emits_record_published_event(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_deposition_srn: DepositionSRN,
        sample_metadata: dict,
    ):
        """Service should emit RecordPublished event."""
        # Arrange
        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
        )

        # Act
        record = await service.publish_record(
            deposition_srn=sample_deposition_srn,
            metadata=sample_metadata,
        )

        # Assert
        mock_outbox.append.assert_called_once()
        event = mock_outbox.append.call_args[0][0]
        assert isinstance(event, RecordPublished)
        assert event.record_srn == record.srn
        assert event.deposition_srn == sample_deposition_srn
        assert event.metadata == sample_metadata

    @pytest.mark.asyncio
    async def test_publish_record_creates_version_1(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_deposition_srn: DepositionSRN,
        sample_metadata: dict,
    ):
        """New records should be version 1."""
        # Arrange
        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
        )

        # Act
        record = await service.publish_record(
            deposition_srn=sample_deposition_srn,
            metadata=sample_metadata,
        )

        # Assert
        assert record.srn.version.root == 1
