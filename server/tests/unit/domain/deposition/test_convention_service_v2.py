"""Unit tests for ConventionService ``deploy`` — inline schema + ingester + event."""

from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError as PydanticValidationError
from tests.factories import make_convention_docs

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.deploy import HookDeploy
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import (
    ColumnDef,
    HookIdentity,
    HookName,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.shared.model.source import IngesterDefinition, IngesterName
from osa.domain.shared.model.srn import (
    ConventionSlug,
    SchemaId,
    SchemaIdentifier,
)


def _make_conv_slug(slug: str = "test-conv") -> ConventionSlug:
    return ConventionSlug(slug)


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


def _make_hook_deploy(name: str = "detect_pockets") -> HookDeploy:
    return HookDeploy(
        identity=HookIdentity(
            name=HookName(name),
            feature=TableFeatureSpec(
                cardinality="many",
                columns=[ColumnDef(name="score", json_type="number", required=True)],
            ),
        ),
        runtime=OciConfig(image="ghcr.io/example/pocketeer", digest="sha256:abc123"),
        source_ref="git+https://example.com/pocketeer@abc",
    )


def _make_ingester_def(**overrides) -> IngesterDefinition:
    kwargs = dict(
        name=IngesterName("rcsb_pdb"),
        image="osa-sources/rcsb-pdb:latest",
        digest="sha256:abc123",
        config={"email": "test@example.com", "batch_size": 100},
        source_ref="git+https://example.com/rcsb-pdb@abc",
    )
    kwargs.update(overrides)
    return IngesterDefinition(**kwargs)


def _make_service(
    conv_repo: AsyncMock | None = None,
    schema_service: AsyncMock | None = None,
    outbox: AsyncMock | None = None,
    hook_registry: AsyncMock | None = None,
    ingester_registry: AsyncMock | None = None,
) -> ConventionService:
    """Create a ConventionService with mock deps."""
    mock_schema_service = schema_service or AsyncMock()
    if not schema_service:
        mock_schema = AsyncMock()
        mock_schema.id = SchemaId.parse("testschema12345678@1.0.0")
        mock_schema.fields = []
        mock_schema_service.create_schema.return_value = mock_schema
        # No existing schema → deploy proceeds to create_schema.
        mock_schema_service.get_schema.side_effect = NotFoundError("schema not found")

    return ConventionService(
        convention_repo=conv_repo or AsyncMock(),
        schema_service=mock_schema_service,
        metadata_service=AsyncMock(),
        hook_registry=hook_registry or AsyncMock(),
        ingester_registry=ingester_registry or AsyncMock(),
        outbox=outbox or AsyncMock(),
    )


async def _deploy(service: ConventionService, **overrides):
    kwargs = dict(
        slug=_make_conv_slug(),
        title="PDB Structures",
        description="Protein structures from the PDB",
        file_requirements=_make_file_reqs(),
        schema_slug=SchemaIdentifier("test-schema"),
        schema_version="1.0.0",
        schema_fields=_make_field_defs(),
        hooks=None,
        docs=make_convention_docs(),
    )
    kwargs.update(overrides)
    return await service.deploy(**kwargs)


class TestDeployWithInlineSchema:
    @pytest.mark.asyncio
    async def test_creates_schema_from_field_definitions(self):
        schema_service = AsyncMock()
        mock_schema = AsyncMock()
        mock_schema.id = SchemaId.parse("testschema12345678@1.0.0")
        mock_schema.fields = []
        schema_service.create_schema.return_value = mock_schema
        schema_service.get_schema.side_effect = NotFoundError("not found")

        service = _make_service(schema_service=schema_service)
        await _deploy(service, title="PDB Structures")
        # SchemaService.create_schema should have been called with field defs
        schema_service.create_schema.assert_called_once()
        call_kwargs = schema_service.create_schema.call_args
        assert call_kwargs[1]["title"] == "PDB Structures"
        assert call_kwargs[1]["version"] == "1.0.0"
        assert len(call_kwargs[1]["fields"]) == 2

    @pytest.mark.asyncio
    async def test_convention_references_created_schema_id(self):
        schema_service = AsyncMock()
        schema_id = SchemaId.parse("created123456789@1.0.0")
        mock_schema = AsyncMock()
        mock_schema.id = schema_id
        mock_schema.fields = []
        schema_service.create_schema.return_value = mock_schema
        schema_service.get_schema.side_effect = NotFoundError("not found")

        service = _make_service(schema_service=schema_service)
        result = await _deploy(service, title="Test")
        assert result.schema_id == schema_id

    @pytest.mark.asyncio
    async def test_convention_saves_ingester_definition(self):
        service = _make_service()
        ingester = _make_ingester_def()
        result = await _deploy(service, title="With Ingester", ingester=ingester)
        assert result.ingester is not None
        assert result.ingester.image == "osa-sources/rcsb-pdb:latest"
        assert result.ingester.digest == "sha256:abc123"
        assert result.ingester.config == {"email": "test@example.com", "batch_size": 100}

    @pytest.mark.asyncio
    async def test_convention_ingester_defaults_to_none(self):
        service = _make_service()
        result = await _deploy(service, title="No Ingester")
        assert result.ingester is None

    @pytest.mark.asyncio
    async def test_convention_with_hooks_emits_hooks_in_event(self):
        outbox = AsyncMock()
        service = _make_service(outbox=outbox)
        hooks = [_make_hook_deploy()]
        await _deploy(service, title="With Hooks", hooks=hooks)
        emitted = outbox.append.call_args[0][0]
        assert len(emitted.hooks) == 1
        assert emitted.hooks[0].name.root == "detect_pockets"


class TestConventionRegisteredEvent:
    @pytest.mark.asyncio
    async def test_deploy_emits_convention_registered(self):
        outbox = AsyncMock()
        service = _make_service(outbox=outbox)
        result = await _deploy(service, title="With Source", ingester=_make_ingester_def())
        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ConventionRegistered)
        assert emitted.convention_id == result.id

    @pytest.mark.asyncio
    async def test_deploy_without_source_still_emits_event(self):
        outbox = AsyncMock()
        service = _make_service(outbox=outbox)
        result = await _deploy(service, title="No Source")
        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ConventionRegistered)
        assert emitted.convention_id == result.id


class TestDeployMintsIngesterRelease:
    """Deploy wires the ingester registry (#180 §1): a declared ingester gets an
    identity + release minted in the same deploy, unconditionally — greenfield,
    no unnamed/pre-registry path. Name and source_ref are required at the model
    boundary, so the only deploy-time behavior to test is the minting itself."""

    def _ingester(self, **overrides) -> IngesterDefinition:
        kwargs = dict(
            name=IngesterName("from_pdb"),
            image="ghcr.io/example/ingester:v1",
            digest="sha256:abc123",
            source_ref="git+https://example.com/repo@deadbeef",
        )
        kwargs.update(overrides)
        return IngesterDefinition(**kwargs)

    @pytest.mark.asyncio
    async def test_declared_ingester_mints_identity_and_release(self):
        registry = AsyncMock()
        service = _make_service(ingester_registry=registry)
        await _deploy(service, ingester=self._ingester(), built_by="ci@example")

        registry.upsert_identity.assert_awaited_once()
        name_arg, schema_arg = registry.upsert_identity.await_args[0]
        assert name_arg == IngesterName("from_pdb")
        assert schema_arg.root == "testschema12345678"

        registry.create_release.assert_awaited_once()
        rel_args = registry.create_release.await_args
        assert rel_args[0][0] == IngesterName("from_pdb")
        runtime = rel_args[0][1]
        assert runtime.image == "ghcr.io/example/ingester:v1"
        assert runtime.digest == "sha256:abc123"
        assert rel_args[0][2] == "git+https://example.com/repo@deadbeef"
        assert rel_args[0][3] == "ci@example"

    @pytest.mark.asyncio
    async def test_no_ingester_mints_nothing(self):
        registry = AsyncMock()
        service = _make_service(ingester_registry=registry)
        await _deploy(service, ingester=None)
        registry.upsert_identity.assert_not_awaited()
        registry.create_release.assert_not_awaited()

    def test_nameless_ingester_is_unrepresentable(self):
        """Greenfield: a declared ingester IS an identity — pydantic rejects the
        nameless form at construction, so no service-level guard can exist."""
        with pytest.raises(PydanticValidationError):
            IngesterDefinition(image="ghcr.io/x:v1", digest="sha256:def456")

    def test_ingester_without_source_ref_is_unrepresentable(self):
        with pytest.raises(PydanticValidationError):
            self._ingester(source_ref=None)
