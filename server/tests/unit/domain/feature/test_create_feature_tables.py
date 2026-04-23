"""Unit tests for CreateFeatureTables event handler."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.feature.handler.create_feature_tables import CreateFeatureTables
from osa.domain.shared.error import ConflictError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.shared.model.srn import ConventionSRN, SchemaId


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_schema_id() -> SchemaId:
    return SchemaId.parse("test@1.0.0")


def _make_hook_definition(name: str = "pocket_detect") -> HookDefinition:
    return HookDefinition(
        name=name,
        runtime=OciConfig(
            image="ghcr.io/example/hook",
            digest="sha256:abc123",
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


def _make_event(hooks: list[HookDefinition] | None = None) -> ConventionRegistered:
    return ConventionRegistered(
        id=EventId(uuid4()),
        convention_srn=_make_conv_srn(),
        schema_id=_make_schema_id(),
        schema_fields=[],
        hooks=hooks or [],
    )


class TestCreateFeatureTables:
    @pytest.mark.asyncio
    async def test_creates_tables_for_each_hook(self):
        hook = _make_hook_definition()
        event = _make_event(hooks=[hook])

        feature_service = AsyncMock()
        handler = CreateFeatureTables(feature_service=feature_service)
        await handler.handle(event)

        feature_service.create_table.assert_called_once_with(hook)

    @pytest.mark.asyncio
    async def test_creates_multiple_tables(self):
        hooks = [_make_hook_definition("hook_a"), _make_hook_definition("hook_b")]
        event = _make_event(hooks=hooks)

        feature_service = AsyncMock()
        handler = CreateFeatureTables(feature_service=feature_service)
        await handler.handle(event)

        assert feature_service.create_table.call_count == 2

    @pytest.mark.asyncio
    async def test_no_hooks_is_noop(self):
        event = _make_event(hooks=[])

        feature_service = AsyncMock()
        handler = CreateFeatureTables(feature_service=feature_service)
        await handler.handle(event)

        feature_service.create_table.assert_not_called()

    @pytest.mark.asyncio
    async def test_propagates_non_conflict_errors(self):
        hook = _make_hook_definition()
        event = _make_event(hooks=[hook])

        feature_service = AsyncMock()
        feature_service.create_table.side_effect = RuntimeError("DDL failed")
        handler = CreateFeatureTables(feature_service=feature_service)

        with pytest.raises(RuntimeError, match="DDL failed"):
            await handler.handle(event)

    @pytest.mark.asyncio
    async def test_skips_existing_tables_on_redelivery(self):
        hooks = [_make_hook_definition("hook_a"), _make_hook_definition("hook_b")]
        event = _make_event(hooks=hooks)

        feature_service = AsyncMock()
        feature_service.create_table.side_effect = ConflictError("table already exists")
        handler = CreateFeatureTables(feature_service=feature_service)
        await handler.handle(event)

        assert feature_service.create_table.call_count == 2
