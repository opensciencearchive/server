"""Unit tests for CreateFeatureTables event handler.

Tests for User Story 2: Convention Initialization Chain.
"""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.feature.event.convention_ready import ConventionReady
from osa.domain.feature.handler.create_feature_tables import CreateFeatureTables
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_hook_snapshot(name: str = "pocket_detect") -> HookSnapshot:
    return HookSnapshot(
        name=name,
        image="ghcr.io/example/hook",
        features=[ColumnDef(name="score", json_type="number", required=True)],
    )


def _make_event(hooks: list[HookSnapshot] | None = None) -> ConventionRegistered:
    return ConventionRegistered(
        id=EventId(uuid4()),
        convention_srn=_make_conv_srn(),
        hooks=hooks or [],
    )


class TestCreateFeatureTables:
    @pytest.mark.asyncio
    async def test_creates_tables_and_emits_convention_ready(self):
        """Given ConventionRegistered with hooks, creates feature tables and emits ConventionReady."""
        hook = _make_hook_snapshot()
        event = _make_event(hooks=[hook])

        feature_service = AsyncMock()
        outbox = AsyncMock()

        handler = CreateFeatureTables(
            feature_service=feature_service,
            outbox=outbox,
        )
        await handler.handle(event)

        feature_service.create_table_from_snapshot.assert_called_once_with(hook)
        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ConventionReady)
        assert emitted.convention_srn == event.convention_srn

    @pytest.mark.asyncio
    async def test_creates_multiple_tables(self):
        """Creates a feature table for each hook in the event."""
        hooks = [_make_hook_snapshot("hook_a"), _make_hook_snapshot("hook_b")]
        event = _make_event(hooks=hooks)

        feature_service = AsyncMock()
        outbox = AsyncMock()

        handler = CreateFeatureTables(
            feature_service=feature_service,
            outbox=outbox,
        )
        await handler.handle(event)

        assert feature_service.create_table_from_snapshot.call_count == 2
        outbox.append.assert_called_once()

    @pytest.mark.asyncio
    async def test_emits_convention_ready_with_empty_hooks(self):
        """Given empty hooks, still emits ConventionReady."""
        event = _make_event(hooks=[])

        feature_service = AsyncMock()
        outbox = AsyncMock()

        handler = CreateFeatureTables(
            feature_service=feature_service,
            outbox=outbox,
        )
        await handler.handle(event)

        feature_service.create_table_from_snapshot.assert_not_called()
        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ConventionReady)

    @pytest.mark.asyncio
    async def test_does_not_emit_convention_ready_on_failure(self):
        """Feature table creation failure does not emit ConventionReady."""
        hook = _make_hook_snapshot()
        event = _make_event(hooks=[hook])

        feature_service = AsyncMock()
        feature_service.create_table_from_snapshot.side_effect = RuntimeError("DDL failed")
        outbox = AsyncMock()

        handler = CreateFeatureTables(
            feature_service=feature_service,
            outbox=outbox,
        )

        with pytest.raises(RuntimeError, match="DDL failed"):
            await handler.handle(event)

        outbox.append.assert_not_called()
