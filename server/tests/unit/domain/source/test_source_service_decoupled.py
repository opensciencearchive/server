"""Unit tests for decoupled SourceService.

Tests for User Story 3: Cross-domain decoupling.
Verifies SourceService emits SourceRecordReady per record
instead of calling DepositionService directly.
"""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.source.event.source_record_ready import SourceRecordReady
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


class TestDecoupledSourceService:
    @pytest.mark.asyncio
    async def test_emits_source_record_ready_per_record(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        """SourceService emits SourceRecordReady for each record."""
        service = SourceService(
            source_runner=mock_source_runner,
            source_storage=mock_source_storage,
            outbox=mock_outbox,
        )
        await service.run_source(
            convention_srn=_make_conv_srn(),
            source=_make_source_def(),
        )

        # 2 SourceRecordReady + 1 SourceRunCompleted = 3 events
        assert mock_outbox.append.call_count == 3
        first = mock_outbox.append.call_args_list[0][0][0]
        second = mock_outbox.append.call_args_list[1][0][0]
        assert isinstance(first, SourceRecordReady)
        assert isinstance(second, SourceRecordReady)
        assert first.source_id == "4HHB"
        assert second.source_id == "1CRN"

    @pytest.mark.asyncio
    async def test_source_record_ready_carries_staging_dir(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        """SourceRecordReady carries the staging_dir path."""
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
    async def test_emits_completion_event(
        self, mock_outbox, mock_source_storage, mock_source_runner
    ):
        """Still emits SourceRunCompleted after all records."""
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

    @pytest.mark.asyncio
    async def test_no_deposition_service_dependency(self):
        """SourceService no longer depends on DepositionService."""
        import inspect

        sig = inspect.signature(SourceService.__init__)
        param_names = list(sig.parameters.keys())
        assert "deposition_service" not in param_names
        assert "convention_repo" not in param_names
