"""Unit tests for ConventionService with inline schema creation and source fields."""

from unittest.mock import AsyncMock

import pytest

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookManifest,
)
from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import Domain, SchemaSRN


def _make_field_defs() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="pdb_id",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="resolution",
            type=FieldType.NUMBER,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _make_file_reqs() -> FileRequirements:
    return FileRequirements(
        accepted_types=[".cif"],
        min_count=1,
        max_count=5,
        max_file_size=500_000_000,
    )


def _make_hook_def(name: str = "detect_pockets") -> HookDefinition:
    return HookDefinition(
        image="ghcr.io/example/pocketeer",
        digest="sha256:abc123",
        manifest=HookManifest(
            name=name,
            record_schema="PDBStructure",
            cardinality="many",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="score", json_type="number", required=True),
                ]
            ),
        ),
    )


def _make_source_def() -> SourceDefinition:
    return SourceDefinition(
        image="osa-sources/rcsb-pdb:latest",
        digest="sha256:abc123",
        config={"email": "test@example.com", "batch_size": 100},
    )


def _make_service(
    conv_repo: AsyncMock | None = None,
    schema_service: AsyncMock | None = None,
    feature_service: AsyncMock | None = None,
    outbox: AsyncMock | None = None,
) -> ConventionService:
    """Create a ConventionService with mock deps."""
    mock_schema_service = schema_service or AsyncMock()
    # Default: create_schema returns a Schema-like obj with .srn
    if not schema_service:
        mock_schema = AsyncMock()
        mock_schema.srn = SchemaSRN.parse("urn:osa:localhost:schema:testschema12345678@1.0.0")
        mock_schema_service.create_schema.return_value = mock_schema

    return ConventionService(
        convention_repo=conv_repo or AsyncMock(),
        schema_service=mock_schema_service,
        feature_service=feature_service or AsyncMock(),
        outbox=outbox or AsyncMock(),
        node_domain=Domain("localhost"),
    )


class TestCreateConventionWithInlineSchema:
    @pytest.mark.asyncio
    async def test_creates_schema_from_field_definitions(self):
        schema_service = AsyncMock()
        mock_schema = AsyncMock()
        mock_schema.srn = SchemaSRN.parse("urn:osa:localhost:schema:testschema12345678@1.0.0")
        schema_service.create_schema.return_value = mock_schema

        service = _make_service(schema_service=schema_service)
        await service.create_convention(
            title="PDB Structures",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        # SchemaService.create_schema should have been called with field defs
        schema_service.create_schema.assert_called_once()
        call_kwargs = schema_service.create_schema.call_args
        assert call_kwargs[1]["title"] == "PDB Structures"
        assert call_kwargs[1]["version"] == "1.0.0"
        assert len(call_kwargs[1]["fields"]) == 2

    @pytest.mark.asyncio
    async def test_convention_references_created_schema_srn(self):
        schema_service = AsyncMock()
        schema_srn = SchemaSRN.parse("urn:osa:localhost:schema:created123456789@1.0.0")
        mock_schema = AsyncMock()
        mock_schema.srn = schema_srn
        schema_service.create_schema.return_value = mock_schema

        service = _make_service(schema_service=schema_service)
        result = await service.create_convention(
            title="Test",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        assert result.schema_srn == schema_srn

    @pytest.mark.asyncio
    async def test_convention_saves_source_definition(self):
        service = _make_service()
        source = _make_source_def()
        result = await service.create_convention(
            title="With Source",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
            source=source,
        )
        assert result.source is not None
        assert result.source.image == "osa-sources/rcsb-pdb:latest"
        assert result.source.digest == "sha256:abc123"
        assert result.source.config == {"email": "test@example.com", "batch_size": 100}

    @pytest.mark.asyncio
    async def test_convention_source_defaults_to_none(self):
        service = _make_service()
        result = await service.create_convention(
            title="No Source",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        assert result.source is None

    @pytest.mark.asyncio
    async def test_convention_with_hooks_creates_feature_tables(self):
        feature_service = AsyncMock()
        service = _make_service(feature_service=feature_service)
        hooks = [_make_hook_def()]
        await service.create_convention(
            title="With Hooks",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
            hooks=hooks,
        )
        feature_service.create_table.assert_called_once()


class TestConventionRegisteredEvent:
    @pytest.mark.asyncio
    async def test_create_convention_emits_convention_registered(self):
        outbox = AsyncMock()
        service = _make_service(outbox=outbox)
        result = await service.create_convention(
            title="With Source",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
            source=_make_source_def(),
        )
        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ConventionRegistered)
        assert emitted.convention_srn == result.srn

    @pytest.mark.asyncio
    async def test_create_convention_without_source_still_emits_event(self):
        outbox = AsyncMock()
        service = _make_service(outbox=outbox)
        result = await service.create_convention(
            title="No Source",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ConventionRegistered)
        assert emitted.convention_srn == result.srn
