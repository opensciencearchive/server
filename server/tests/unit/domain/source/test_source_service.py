"""Unit tests for SourceService with OCI container model."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, SchemaSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.source.port.source_runner import SourceOutput
from osa.domain.source.service.source import SourceService


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test-conv-12345678@1.0.0")


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep-12345678")


def _make_source_def() -> SourceDefinition:
    return SourceDefinition(
        image="osa-sources/test:latest",
        digest="sha256:abc123",
        config={"batch_size": 100},
    )


def _make_convention(source: SourceDefinition | None = None) -> Convention:
    return Convention(
        srn=_make_conv_srn(),
        title="Test Convention",
        schema_srn=SchemaSRN.parse("urn:osa:localhost:schema:test@1.0.0"),
        file_requirements=FileRequirements(
            accepted_types=[".cif"], min_count=0, max_count=5, max_file_size=500_000_000
        ),
        hooks=[],
        source=source,
        created_at=datetime.now(UTC),
    )


@pytest.fixture
def mock_outbox() -> Outbox:
    outbox = MagicMock(spec=Outbox)
    outbox.append = AsyncMock()
    return outbox


@pytest.fixture
def mock_deposition_service() -> AsyncMock:
    dep_service = AsyncMock()
    dep = MagicMock(spec=Deposition)
    dep.srn = _make_dep_srn()
    dep_service.create.return_value = dep
    dep_service.update_metadata.return_value = dep
    dep_service.submit.return_value = dep
    return dep_service


@pytest.fixture
def mock_convention_repo() -> AsyncMock:
    repo = AsyncMock()
    repo.get.return_value = _make_convention(source=_make_source_def())
    return repo


@pytest.fixture
def mock_file_storage() -> MagicMock:
    storage = MagicMock()
    storage.get_source_staging_dir.return_value = Path("/tmp/staging")
    storage.get_source_output_dir.return_value = Path("/tmp/output")
    storage.move_source_files_to_deposition.return_value = None
    return storage


@pytest.fixture
def mock_source_runner() -> AsyncMock:
    runner = AsyncMock()
    runner.run.return_value = SourceOutput(
        records=[
            {"source_id": "4HHB", "metadata": {"pdb_id": "4HHB", "title": "Hemoglobin"}},
            {"source_id": "1CRN", "metadata": {"pdb_id": "1CRN", "title": "Crambin"}},
        ],
        session=None,
        files_dir=Path("/tmp/staging"),
    )
    return runner


class TestSourceService:
    @pytest.mark.asyncio
    async def test_run_source_creates_depositions(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        result = await service.run_source(convention_srn=_make_conv_srn())
        assert result.record_count == 2
        assert mock_deposition_service.create.call_count == 2

    @pytest.mark.asyncio
    async def test_run_source_moves_files(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        await service.run_source(convention_srn=_make_conv_srn())
        assert mock_file_storage.move_source_files_to_deposition.call_count == 2

    @pytest.mark.asyncio
    async def test_run_source_submits_each(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        await service.run_source(convention_srn=_make_conv_srn())
        assert mock_deposition_service.submit.call_count == 2

    @pytest.mark.asyncio
    async def test_run_source_uses_system_user(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        from osa.domain.auth.model.value import SYSTEM_USER_ID

        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        await service.run_source(convention_srn=_make_conv_srn())
        call_kwargs = mock_deposition_service.create.call_args_list[0]
        assert call_kwargs[1]["owner_id"] == SYSTEM_USER_ID

    @pytest.mark.asyncio
    async def test_run_source_emits_completion_event(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        from osa.domain.source.event.source_run_completed import SourceRunCompleted

        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        await service.run_source(convention_srn=_make_conv_srn())
        last_call = mock_outbox.append.call_args_list[-1]
        event = last_call[0][0]
        assert isinstance(event, SourceRunCompleted)
        assert event.record_count == 2
        assert event.convention_srn == _make_conv_srn()
        assert event.is_final_chunk is True

    @pytest.mark.asyncio
    async def test_run_source_emits_continuation_when_session(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        from osa.domain.source.event.source_requested import SourceRequested

        mock_source_runner.run.return_value = SourceOutput(
            records=[{"source_id": "4HHB", "metadata": {"pdb_id": "4HHB"}}],
            session={"cursor": "abc"},
            files_dir=Path("/tmp/staging"),
        )
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        await service.run_source(convention_srn=_make_conv_srn())
        # Should have emitted continuation + completion = 2 events
        assert mock_outbox.append.call_count == 2
        first_event = mock_outbox.append.call_args_list[0][0][0]
        assert isinstance(first_event, SourceRequested)
        assert first_event.session == {"cursor": "abc"}

    @pytest.mark.asyncio
    async def test_run_source_raises_for_missing_convention(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_file_storage,
        mock_source_runner,
    ):
        convention_repo = AsyncMock()
        convention_repo.get.return_value = None
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=convention_repo,
            outbox=mock_outbox,
        )
        with pytest.raises(ValueError, match="Convention not found"):
            await service.run_source(convention_srn=_make_conv_srn())

    @pytest.mark.asyncio
    async def test_run_source_raises_for_no_source_defined(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_file_storage,
        mock_source_runner,
    ):
        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention(source=None)
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=convention_repo,
            outbox=mock_outbox,
        )
        with pytest.raises(ValueError, match="no source defined"):
            await service.run_source(convention_srn=_make_conv_srn())

    @pytest.mark.asyncio
    async def test_run_source_final_when_session_but_zero_records(
        self,
        mock_outbox,
        mock_deposition_service,
        mock_convention_repo,
        mock_file_storage,
        mock_source_runner,
    ):
        """Source returns session but zero records → treated as final chunk."""
        from osa.domain.source.event.source_run_completed import SourceRunCompleted

        mock_source_runner.run.return_value = SourceOutput(
            records=[],
            session={"cursor": "x"},
            files_dir=Path("/tmp/staging"),
        )
        service = SourceService(
            source_runner=mock_source_runner,
            deposition_service=mock_deposition_service,
            file_storage=mock_file_storage,
            convention_repo=mock_convention_repo,
            outbox=mock_outbox,
        )
        await service.run_source(convention_srn=_make_conv_srn())

        # Only one event (SourceRunCompleted), no continuation
        assert mock_outbox.append.call_count == 1
        event = mock_outbox.append.call_args_list[0][0][0]
        assert isinstance(event, SourceRunCompleted)
        assert event.is_final_chunk is True
