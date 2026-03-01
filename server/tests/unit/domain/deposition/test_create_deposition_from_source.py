"""Unit tests for CreateDepositionFromSource event handler.

Tests for User Story 3: Cross-domain decoupling.
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.deposition.handler.create_deposition_from_source import (
    CreateDepositionFromSource,
)
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.source.event.source_record_ready import SourceRecordReady


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_event() -> SourceRecordReady:
    return SourceRecordReady(
        id=EventId(uuid4()),
        convention_srn=_make_conv_srn(),
        metadata={"pdb_id": "4HHB", "title": "Hemoglobin"},
        file_paths=["4HHB/structure.cif"],
        source_id="4HHB",
        staging_dir="/tmp/staging/run-123",
    )


class TestCreateDepositionFromSource:
    @pytest.mark.asyncio
    async def test_creates_deposition_and_submits(self):
        """Handler creates deposition, updates metadata, moves files, and submits."""
        dep = MagicMock(spec=Deposition)
        dep.srn = _make_dep_srn()

        deposition_service = AsyncMock()
        deposition_service.create.return_value = dep
        file_storage = MagicMock()

        handler = CreateDepositionFromSource(
            deposition_service=deposition_service,
            file_storage=file_storage,
        )
        event = _make_event()
        await handler.handle(event)

        # Creates deposition
        deposition_service.create.assert_called_once()
        create_kwargs = deposition_service.create.call_args[1]
        assert create_kwargs["convention_srn"] == event.convention_srn

        # Updates metadata
        deposition_service.update_metadata.assert_called_once_with(
            srn=dep.srn,
            metadata=event.metadata,
        )

        # Moves files
        file_storage.move_source_files_to_deposition.assert_called_once()

        # Submits
        deposition_service.submit.assert_called_once_with(srn=dep.srn)

    @pytest.mark.asyncio
    async def test_uses_system_user_id(self):
        """Handler creates deposition with SYSTEM_USER_ID."""
        from osa.domain.auth.model.value import SYSTEM_USER_ID

        dep = MagicMock(spec=Deposition)
        dep.srn = _make_dep_srn()

        deposition_service = AsyncMock()
        deposition_service.create.return_value = dep
        file_storage = MagicMock()

        handler = CreateDepositionFromSource(
            deposition_service=deposition_service,
            file_storage=file_storage,
        )
        await handler.handle(_make_event())

        create_kwargs = deposition_service.create.call_args[1]
        assert create_kwargs["owner_id"] == SYSTEM_USER_ID
