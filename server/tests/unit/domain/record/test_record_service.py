"""Unit tests for RecordService."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.service.record import RecordService
from osa.domain.shared.model.source import (
    DepositionSource,
    HarvestSource,
)
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, Domain, LocalId
from osa.domain.shared.outbox import Outbox


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


@pytest.fixture
def mock_record_repo() -> RecordRepository:
    repo = MagicMock(spec=RecordRepository)
    repo.save = AsyncMock()
    return repo


@pytest.fixture
def mock_outbox() -> Outbox:
    outbox = MagicMock(spec=Outbox)
    outbox.append = AsyncMock()
    return outbox


@pytest.fixture
def node_domain() -> Domain:
    return Domain("test.example.com")


@pytest.fixture
def sample_draft(node_domain: Domain) -> RecordDraft:
    dep_srn = DepositionSRN(domain=node_domain, id=LocalId(str(uuid4())))
    return RecordDraft(
        source=DepositionSource(id=str(dep_srn)),
        metadata={"title": "Test Record", "organism": "human", "platform": "GPL570"},
        convention_srn=_make_conv_srn(),
        expected_features=["pocket_detect"],
    )


class TestRecordService:
    @pytest.mark.asyncio
    async def test_publish_record_creates_record(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_draft: RecordDraft,
    ):
        """Service should create and persist a Record from a draft."""
        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
            feature_reader=AsyncMock(),
        )

        record = await service.publish_record(sample_draft)

        assert record is not None
        assert record.source == sample_draft.source
        assert record.convention_srn == sample_draft.convention_srn
        assert record.metadata == sample_draft.metadata
        mock_record_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_record_emits_record_published_event(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_draft: RecordDraft,
    ):
        """Service should emit RecordPublished event with source-agnostic fields."""
        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
            feature_reader=AsyncMock(),
        )

        record = await service.publish_record(sample_draft)

        mock_outbox.append.assert_called_once()
        event = mock_outbox.append.call_args[0][0]
        assert isinstance(event, RecordPublished)
        assert event.record_srn == record.srn
        assert event.source == sample_draft.source
        assert event.convention_srn == sample_draft.convention_srn
        assert event.expected_features == sample_draft.expected_features
        assert event.metadata == sample_draft.metadata

    @pytest.mark.asyncio
    async def test_publish_record_creates_version_1(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_draft: RecordDraft,
    ):
        """New records should be version 1."""
        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
            feature_reader=AsyncMock(),
        )

        record = await service.publish_record(sample_draft)

        assert record.srn.version.root == 1


class TestRecordServiceHarvestSource:
    """US2: Verify harvest-sourced records publish correctly."""

    @pytest.mark.asyncio
    async def test_publish_with_harvest_source(
        self,
        mock_record_repo: RecordRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
    ):
        """HarvestSource draft produces correct Record + RecordPublished event."""
        draft = RecordDraft(
            source=HarvestSource(
                id="run-123-pdb-456",
                harvest_run_srn="urn:osa:localhost:val:run123",
                upstream_source="pdb",
            ),
            metadata={"title": "Harvested Protein"},
            convention_srn=_make_conv_srn(),
            expected_features=["pocket_detect"],
        )

        service = RecordService(
            record_repo=mock_record_repo,
            outbox=mock_outbox,
            node_domain=node_domain,
            feature_reader=AsyncMock(),
        )

        record = await service.publish_record(draft)

        assert record.source.type == "harvest"
        assert record.source.upstream_source == "pdb"
        assert record.convention_srn == _make_conv_srn()
        mock_record_repo.save.assert_called_once()

        event = mock_outbox.append.call_args[0][0]
        assert isinstance(event, RecordPublished)
        assert event.source.type == "harvest"
        assert event.expected_features == ["pocket_detect"]
