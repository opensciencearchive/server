"""Unit tests for ConventionService (#145 bundled ``deploy``)."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from tests.factories import make_convention_docs

from osa.domain.deposition.model.convention import Convention
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
from osa.domain.shared.model.srn import (
    ConventionSlug,
    SchemaId,
    SchemaIdentifier,
)


def _make_conv_slug(slug: str = "test-conv") -> ConventionSlug:
    return ConventionSlug(slug)


def _make_schema_id(id: str = "test-schema", version: str = "1.0.0") -> SchemaId:
    return SchemaId.parse(f"{id}@{version}")


def _make_field_defs() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="title",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _make_file_reqs() -> FileRequirements:
    return FileRequirements(
        accepted_types=[".csv"],
        min_count=1,
        max_count=3,
        max_file_size=1_000_000,
    )


def _make_hook_deploy(name: str = "pocket_detect") -> HookDeploy:
    return HookDeploy(
        identity=HookIdentity(
            name=HookName(name),
            feature=TableFeatureSpec(
                cardinality="one",
                columns=[ColumnDef(name="score", json_type="number", required=True)],
            ),
        ),
        runtime=OciConfig(image="ghcr.io/example/hook", digest="sha256:abc123"),
        source_ref="git+https://example.com/hook@abc",
    )


def _make_service(
    conv_repo: AsyncMock | None = None,
    schema_service: AsyncMock | None = None,
    outbox: AsyncMock | None = None,
    hook_registry: AsyncMock | None = None,
) -> ConventionService:
    mock_schema_service = schema_service or AsyncMock()
    if not schema_service:
        mock_schema = AsyncMock()
        mock_schema.id = _make_schema_id()
        mock_schema.fields = []
        mock_schema_service.create_schema.return_value = mock_schema
        # No existing schema → deploy proceeds to create_schema.
        mock_schema_service.get_schema.side_effect = NotFoundError("schema not found")

    return ConventionService(
        convention_repo=conv_repo or AsyncMock(),
        schema_service=mock_schema_service,
        metadata_service=AsyncMock(),
        hook_registry=hook_registry or AsyncMock(),
        outbox=outbox or AsyncMock(),
    )


async def _deploy(service: ConventionService, **overrides) -> Convention:
    kwargs = dict(
        slug=_make_conv_slug(),
        title="Test Convention",
        description="A test convention",
        file_requirements=_make_file_reqs(),
        schema_slug=SchemaIdentifier("test-schema"),
        schema_version="1.0.0",
        schema_fields=_make_field_defs(),
        hooks=None,
        docs=make_convention_docs(),
    )
    kwargs.update(overrides)
    return await service.deploy(**kwargs)


class TestConventionServiceDeploy:
    @pytest.mark.asyncio
    async def test_deploy_creates_schema(self):
        conv_repo = AsyncMock()
        schema_service = AsyncMock()
        mock_schema = AsyncMock()
        mock_schema.id = _make_schema_id()
        mock_schema.fields = []
        schema_service.create_schema.return_value = mock_schema
        schema_service.get_schema.side_effect = NotFoundError("not found")

        service = _make_service(conv_repo, schema_service)
        result = await _deploy(service, title="Test Convention")
        assert result.title == "Test Convention"
        conv_repo.save.assert_called_once()
        schema_service.create_schema.assert_called_once()

    @pytest.mark.asyncio
    async def test_deploy_returns_slug_identity(self):
        service = _make_service()
        result = await _deploy(service, slug=_make_conv_slug("my-conv"))
        assert result.id.root == "my-conv"

    @pytest.mark.asyncio
    async def test_deploy_with_hooks_emits_hooks_in_event(self):
        outbox = AsyncMock()
        hook_registry = AsyncMock()
        service = _make_service(outbox=outbox, hook_registry=hook_registry)
        hooks = [_make_hook_deploy()]
        result = await _deploy(service, hooks=hooks)
        # Convention references the hook by name.
        assert result.hooks == [HookName("pocket_detect")]
        # The registry was asked to upsert the identity + mint the release.
        hook_registry.upsert_identity.assert_called_once()
        hook_registry.create_release.assert_called_once()
        # The emitted event carries hook identities directly.
        emitted = outbox.append.call_args[0][0]
        assert len(emitted.hooks) == 1
        assert emitted.hooks[0].name.root == "pocket_detect"

    @pytest.mark.asyncio
    async def test_deploy_without_hooks_emits_empty_hooks(self):
        outbox = AsyncMock()
        service = _make_service(outbox=outbox)
        await _deploy(service, title="No Hooks", hooks=None)
        emitted = outbox.append.call_args[0][0]
        assert emitted.hooks == []

    @pytest.mark.asyncio
    async def test_redeploy_same_slug_is_idempotent_upsert(self):
        # #145: conventions are mutable + unversioned — re-deploying the same
        # slug upserts in place and must NOT raise a conflict.
        conv_repo = AsyncMock()
        service = _make_service(conv_repo=conv_repo)

        first = await _deploy(service, slug=_make_conv_slug("proteins"))
        second = await _deploy(service, slug=_make_conv_slug("proteins"))

        assert first.id == second.id
        assert conv_repo.save.call_count == 2


class TestConventionServiceGet:
    @pytest.mark.asyncio
    async def test_get_existing(self):
        conv = Convention(
            id=_make_conv_slug(),
            title="Test",
            description="A test convention",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            docs=make_convention_docs(),
            created_at=datetime.now(UTC),
        )
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv

        service = _make_service(conv_repo=conv_repo)
        result = await service.get_convention(conv.id)
        assert result == conv

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = None

        service = _make_service(conv_repo=conv_repo)
        with pytest.raises(NotFoundError):
            await service.get_convention(_make_conv_slug())


class TestConventionServiceList:
    @pytest.mark.asyncio
    async def test_list_conventions(self):
        conv = Convention(
            id=_make_conv_slug(),
            title="Test",
            description="A test convention",
            schema_id=_make_schema_id(),
            file_requirements=_make_file_reqs(),
            docs=make_convention_docs(),
            created_at=datetime.now(UTC),
        )
        conv_repo = AsyncMock()
        conv_repo.list.return_value = [conv]

        service = _make_service(conv_repo=conv_repo)
        result = await service.list_conventions()
        assert len(result) == 1
