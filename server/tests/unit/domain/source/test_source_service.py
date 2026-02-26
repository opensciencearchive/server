"""Unit tests for SourceService with OCI container model.

Updated for cross-domain decoupling: SourceService no longer depends on
DepositionService, ConventionRepository, or FileStoragePort. Instead it
uses SourceStoragePort and emits SourceRecordReady events per record.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.source.event.source_record_ready import SourceRecordReady
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted
from osa.domain.source.port.source_runner import SourceOutput
from osa.domain.source.service.source import SourceService


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test-conv-12345678@1.0.0")


def _make_source_def() -> SourceDefinition:
    return SourceDefinition(
        image="osa-sources/test:latest",
        digest="sha256:abc123",
        config={"batch_size": 100},
    )


@pytest.fixture
def mock_outbox() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def mock_source_storage() -> MagicMock:
    storage = MagicMock()
    storage.get_source_staging_dir.return_value = Path("/tmp/staging")
    storage.get_source_output_dir.return_value = Path("/tmp/output")
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
    async def test_run_source_emits_per_record_events(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        service = SourceService(
            source_runner=mock_source_runner,
            source_storage=mock_source_storage,
            outbox=mock_outbox,
        )
        result = await service.run_source(
            convention_srn=_make_conv_srn(),
            source=_make_source_def(),
        )
        assert result.record_count == 2

        # 2 SourceRecordReady + 1 SourceRunCompleted = 3 events
        assert mock_outbox.append.call_count == 3
        first = mock_outbox.append.call_args_list[0][0][0]
        second = mock_outbox.append.call_args_list[1][0][0]
        assert isinstance(first, SourceRecordReady)
        assert isinstance(second, SourceRecordReady)
        assert first.source_id == "4HHB"
        assert second.source_id == "1CRN"

    @pytest.mark.asyncio
    async def test_run_source_carries_staging_dir(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        service = SourceService(
            source_runner=mock_source_runner,
            source_storage=mock_source_storage,
            outbox=mock_outbox,
        )
        await service.run_source(
            convention_srn=_make_conv_srn(),
            source=_make_source_def(),
        )
        event = mock_outbox.append.call_args_list[0][0][0]
        assert isinstance(event, SourceRecordReady)
        assert event.staging_dir == str(Path("/tmp/staging"))

    @pytest.mark.asyncio
    async def test_run_source_emits_completion_event(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        service = SourceService(
            source_runner=mock_source_runner,
            source_storage=mock_source_storage,
            outbox=mock_outbox,
        )
        await service.run_source(
            convention_srn=_make_conv_srn(),
            source=_make_source_def(),
        )
        last = mock_outbox.append.call_args_list[-1][0][0]
        assert isinstance(last, SourceRunCompleted)
        assert last.record_count == 2
        assert last.convention_srn == _make_conv_srn()
        assert last.is_final_chunk is True

    @pytest.mark.asyncio
    async def test_run_source_emits_continuation_when_session(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        mock_source_runner.run.return_value = SourceOutput(
            records=[{"source_id": "4HHB", "metadata": {"pdb_id": "4HHB"}}],
            session={"cursor": "abc"},
            files_dir=Path("/tmp/staging"),
        )
        service = SourceService(
            source_runner=mock_source_runner,
            source_storage=mock_source_storage,
            outbox=mock_outbox,
        )
        await service.run_source(
            convention_srn=_make_conv_srn(),
            source=_make_source_def(),
        )
        # 1 SourceRecordReady + 1 SourceRequested continuation + 1 SourceRunCompleted = 3 events
        assert mock_outbox.append.call_count == 3
        continuation = mock_outbox.append.call_args_list[1][0][0]
        assert isinstance(continuation, SourceRequested)
        assert continuation.session == {"cursor": "abc"}

    @pytest.mark.asyncio
    async def test_run_source_final_when_session_but_zero_records(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        """Source returns session but zero records -> treated as final chunk."""
        mock_source_runner.run.return_value = SourceOutput(
            records=[],
            session={"cursor": "x"},
            files_dir=Path("/tmp/staging"),
        )
        service = SourceService(
            source_runner=mock_source_runner,
            source_storage=mock_source_storage,
            outbox=mock_outbox,
        )
        await service.run_source(
            convention_srn=_make_conv_srn(),
            source=_make_source_def(),
        )
        # Only SourceRunCompleted, no continuation
        assert mock_outbox.append.call_count == 1
        event = mock_outbox.append.call_args_list[0][0][0]
        assert isinstance(event, SourceRunCompleted)
        assert event.is_final_chunk is True
