"""Tests for TriggerSourceOnDeploy event handler."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import InitialRunConfig, SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.handler.trigger_source_on_deploy import TriggerSourceOnDeploy


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test-deploy-conv@1.0.0")


def _make_schema_srn() -> SchemaSRN:
    return SchemaSRN.parse("urn:osa:localhost:schema:test-schema12345678@1.0.0")


def _make_file_reqs() -> FileRequirements:
    return FileRequirements(
        accepted_types=[".csv"],
        min_count=1,
        max_count=3,
        max_file_size=1_000_000,
    )


def _make_convention(
    source: SourceDefinition | None = None,
) -> Convention:
    return Convention(
        srn=_make_conv_srn(),
        title="Test Convention",
        schema_srn=_make_schema_srn(),
        file_requirements=_make_file_reqs(),
        source=source,
        created_at=datetime.now(UTC),
    )


def _make_event() -> ConventionRegistered:
    return ConventionRegistered(
        id=EventId(uuid4()),
        convention_srn=_make_conv_srn(),
    )


class TestTriggerSourceOnDeploy:
    @pytest.mark.asyncio
    async def test_emits_source_requested_when_initial_run_configured(self):
        source = SourceDefinition(
            image="osa-sources/test:latest",
            digest="sha256:abc123",
            initial_run=InitialRunConfig(limit=500),
        )
        convention = _make_convention(source=source)

        convention_service = AsyncMock()
        convention_service.get_convention.return_value = convention
        outbox = AsyncMock()

        handler = TriggerSourceOnDeploy(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_event())

        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, SourceRequested)
        assert emitted.convention_srn == convention.srn
        assert emitted.limit == 500

    @pytest.mark.asyncio
    async def test_no_event_when_source_has_no_initial_run(self):
        source = SourceDefinition(
            image="osa-sources/test:latest",
            digest="sha256:abc123",
            initial_run=None,
        )
        convention = _make_convention(source=source)

        convention_service = AsyncMock()
        convention_service.get_convention.return_value = convention
        outbox = AsyncMock()

        handler = TriggerSourceOnDeploy(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_event())

        outbox.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_event_when_convention_has_no_source(self):
        convention = _make_convention(source=None)

        convention_service = AsyncMock()
        convention_service.get_convention.return_value = convention
        outbox = AsyncMock()

        handler = TriggerSourceOnDeploy(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_event())

        outbox.append.assert_not_called()
