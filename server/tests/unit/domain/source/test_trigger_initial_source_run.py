"""Tests for TriggerInitialSourceRun — verifies per-convention filtering."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.application.event import ServerStarted
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import InitialRunConfig, SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.domain.source.event.source_requested import SourceRequested
from osa.domain.source.event.source_run_completed import SourceRunCompleted
from osa.domain.source.handler.trigger_initial_source_run import TriggerInitialSourceRun


def _make_server_started() -> ServerStarted:
    return ServerStarted(id=EventId(uuid4()))


def _make_convention(
    conv_id: str, *, initial_run: InitialRunConfig | None = InitialRunConfig(limit=100)
) -> Convention:
    return Convention(
        srn=ConventionSRN.parse(f"urn:osa:localhost:conv:{conv_id}@1.0.0"),
        title=f"Convention {conv_id}",
        schema_srn=SchemaSRN.parse("urn:osa:localhost:schema:test@1.0.0"),
        file_requirements=FileRequirements(
            accepted_types=[".csv"], min_count=1, max_count=3, max_file_size=1_000_000
        ),
        source=SourceDefinition(
            image="osa-sources/test:latest",
            digest="sha256:abc123",
            initial_run=initial_run,
        ),
        created_at=datetime.now(UTC),
    )


class TestTriggerInitialSourceRunFiltering:
    """Verify find_latest_where filters by convention, not globally."""

    @pytest.mark.asyncio
    async def test_emits_for_convention_without_prior_run(self):
        """Convention with no prior SourceRunCompleted should emit SourceRequested."""
        conv = _make_convention("conv-aaa")

        convention_service = AsyncMock()
        convention_service.list_conventions_with_source.return_value = [conv]

        outbox = AsyncMock()
        outbox.find_latest_where.return_value = None

        handler = TriggerInitialSourceRun(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_server_started())

        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, SourceRequested)
        assert emitted.convention_srn == conv.srn

    @pytest.mark.asyncio
    async def test_skips_convention_with_prior_run(self):
        """Convention with a prior SourceRunCompleted should be skipped."""
        conv = _make_convention("conv-bbb")

        convention_service = AsyncMock()
        convention_service.list_conventions_with_source.return_value = [conv]

        prior_run = SourceRunCompleted(
            id=EventId(uuid4()),
            convention_srn=conv.srn,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            record_count=10,
        )
        outbox = AsyncMock()
        outbox.find_latest_where.return_value = prior_run

        handler = TriggerInitialSourceRun(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_server_started())

        outbox.append.assert_not_called()

    @pytest.mark.asyncio
    async def test_multi_convention_only_emits_for_new_ones(self):
        """With two conventions, only emit for the one without a prior run."""
        conv_a = _make_convention("conv-aaa")
        conv_b = _make_convention("conv-bbb")

        convention_service = AsyncMock()
        convention_service.list_conventions_with_source.return_value = [conv_a, conv_b]

        # conv_a has a prior run, conv_b does not
        prior_run_a = SourceRunCompleted(
            id=EventId(uuid4()),
            convention_srn=conv_a.srn,
            started_at=datetime.now(UTC),
            completed_at=datetime.now(UTC),
            record_count=5,
        )

        async def fake_find_latest_where(event_type, **filters):
            convention_srn = filters.get("convention_srn")
            if convention_srn == str(conv_a.srn):
                return prior_run_a
            return None

        outbox = AsyncMock()
        outbox.find_latest_where.side_effect = fake_find_latest_where

        handler = TriggerInitialSourceRun(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_server_started())

        # Only conv_b should have emitted
        assert outbox.append.call_count == 1
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, SourceRequested)
        assert emitted.convention_srn == conv_b.srn

    @pytest.mark.asyncio
    async def test_find_latest_where_called_with_convention_srn(self):
        """Verify find_latest_where is called with the correct convention_srn filter."""
        conv = _make_convention("conv-ccc")

        convention_service = AsyncMock()
        convention_service.list_conventions_with_source.return_value = [conv]

        outbox = AsyncMock()
        outbox.find_latest_where.return_value = None

        handler = TriggerInitialSourceRun(
            convention_service=convention_service,
            outbox=outbox,
        )
        await handler.handle(_make_server_started())

        outbox.find_latest_where.assert_called_once_with(
            SourceRunCompleted, convention_srn=str(conv.srn)
        )
