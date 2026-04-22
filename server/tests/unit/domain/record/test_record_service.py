"""Unit tests for RecordService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.port.repository import RecordRepository
from osa.domain.record.service.record import RecordService
from osa.domain.shared.model.source import DepositionSource, IngestSource
from osa.domain.shared.model.srn import (
    ConventionSRN,
    DepositionSRN,
    Domain,
    LocalId,
    SchemaId,
)
from osa.domain.shared.outbox import Outbox


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_schema_id() -> SchemaId:
    return SchemaId.parse("test@1.0.0")


def _make_convention() -> Convention:
    return Convention(
        srn=_make_conv_srn(),
        title="Test Convention",
        description=None,
        schema_id=_make_schema_id(),
        file_requirements=FileRequirements(accepted_types=[], max_count=0, max_file_size=0),
        hooks=[],
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def mock_record_repo() -> RecordRepository:
    repo = MagicMock(spec=RecordRepository)
    repo.save = AsyncMock()
    return repo


@pytest.fixture
def mock_convention_repo() -> ConventionRepository:
    repo = MagicMock(spec=ConventionRepository)
    repo.get = AsyncMock(return_value=_make_convention())
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


def _make_service(
    record_repo: RecordRepository,
    convention_repo: ConventionRepository,
    outbox: Outbox,
    node_domain: Domain,
) -> RecordService:
    return RecordService(
        record_repo=record_repo,
        convention_repo=convention_repo,
        metadata_service=AsyncMock(),
        outbox=outbox,
        node_domain=node_domain,
        feature_reader=AsyncMock(),
    )


class TestRecordService:
    @pytest.mark.asyncio
    async def test_publish_record_creates_record(
        self,
        mock_record_repo: RecordRepository,
        mock_convention_repo: ConventionRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_draft: RecordDraft,
    ):
        service = _make_service(mock_record_repo, mock_convention_repo, mock_outbox, node_domain)

        record = await service.publish_record(sample_draft)

        assert record is not None
        assert record.source == sample_draft.source
        assert record.convention_srn == sample_draft.convention_srn
        assert record.schema_id == _make_schema_id()
        assert record.metadata == sample_draft.metadata
        mock_record_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_record_emits_record_published_event(
        self,
        mock_record_repo: RecordRepository,
        mock_convention_repo: ConventionRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_draft: RecordDraft,
    ):
        service = _make_service(mock_record_repo, mock_convention_repo, mock_outbox, node_domain)

        record = await service.publish_record(sample_draft)

        mock_outbox.append.assert_called_once()
        event = mock_outbox.append.call_args[0][0]
        assert isinstance(event, RecordPublished)
        assert event.record_srn == record.srn
        assert event.source == sample_draft.source
        assert event.convention_srn == sample_draft.convention_srn
        assert event.schema_id == _make_schema_id()
        assert event.expected_features == sample_draft.expected_features
        assert event.metadata == sample_draft.metadata

    @pytest.mark.asyncio
    async def test_publish_record_creates_version_1(
        self,
        mock_record_repo: RecordRepository,
        mock_convention_repo: ConventionRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
        sample_draft: RecordDraft,
    ):
        service = _make_service(mock_record_repo, mock_convention_repo, mock_outbox, node_domain)

        record = await service.publish_record(sample_draft)

        assert record.srn.version.root == 1


class TestRecordServiceIngestSource:
    @pytest.mark.asyncio
    async def test_publish_with_ingest_source(
        self,
        mock_record_repo: RecordRepository,
        mock_convention_repo: ConventionRepository,
        mock_outbox: Outbox,
        node_domain: Domain,
    ):
        draft = RecordDraft(
            source=IngestSource(
                id="run-123-pdb-456",
                ingest_run_id="run123",
                upstream_source="pdb",
            ),
            metadata={"title": "Ingested Protein"},
            convention_srn=_make_conv_srn(),
            expected_features=["pocket_detect"],
        )

        service = _make_service(mock_record_repo, mock_convention_repo, mock_outbox, node_domain)

        record = await service.publish_record(draft)

        assert record.source.type == "ingest"
        assert record.source.upstream_source == "pdb"
        assert record.convention_srn == _make_conv_srn()
        mock_record_repo.save.assert_called_once()

        event = mock_outbox.append.call_args[0][0]
        assert isinstance(event, RecordPublished)
        assert event.source.type == "ingest"
        assert event.expected_features == ["pocket_detect"]
