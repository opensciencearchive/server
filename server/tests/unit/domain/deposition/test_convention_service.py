"""Unit tests for ConventionService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookManifest,
)
from osa.domain.shared.model.srn import ConventionSRN, Domain, SchemaSRN


def _make_conv_srn(id: str = "test-conv", version: str = "1.0.0") -> ConventionSRN:
    return ConventionSRN.parse(f"urn:osa:localhost:conv:{id}@{version}")


def _make_schema_srn(id: str = "test-schema", version: str = "1.0.0") -> SchemaSRN:
    return SchemaSRN.parse(f"urn:osa:localhost:schema:{id}@{version}")


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


def _make_hook_def(name: str = "pocket_detect") -> HookDefinition:
    return HookDefinition(
        image="ghcr.io/example/hook",
        digest="sha256:abc123",
        manifest=HookManifest(
            name=name,
            record_schema="SampleSchema",
            cardinality="one",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="score", json_type="number", required=True),
                ]
            ),
        ),
    )


def _make_service(
    conv_repo: AsyncMock | None = None,
    schema_service: AsyncMock | None = None,
    feature_service: AsyncMock | None = None,
    outbox: AsyncMock | None = None,
) -> ConventionService:
    mock_schema_service = schema_service or AsyncMock()
    if not schema_service:
        mock_schema = AsyncMock()
        mock_schema.srn = _make_schema_srn()
        mock_schema_service.create_schema.return_value = mock_schema

    return ConventionService(
        convention_repo=conv_repo or AsyncMock(),
        schema_service=mock_schema_service,
        feature_service=feature_service or AsyncMock(),
        outbox=outbox or AsyncMock(),
        node_domain=Domain("localhost"),
    )


class TestConventionServiceCreate:
    @pytest.mark.asyncio
    async def test_create_convention_creates_schema(self):
        conv_repo = AsyncMock()
        schema_service = AsyncMock()
        mock_schema = AsyncMock()
        mock_schema.srn = _make_schema_srn()
        schema_service.create_schema.return_value = mock_schema
        feature_service = AsyncMock()

        service = _make_service(conv_repo, schema_service, feature_service)
        result = await service.create_convention(
            title="Test Convention",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        assert result.title == "Test Convention"
        conv_repo.save.assert_called_once()
        schema_service.create_schema.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_convention_generates_srn(self):
        service = _make_service()
        result = await service.create_convention(
            title="Test",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        assert str(result.srn).startswith("urn:osa:localhost:conv:")

    @pytest.mark.asyncio
    async def test_create_convention_with_hooks_creates_feature_tables(self):
        feature_service = AsyncMock()

        service = _make_service(feature_service=feature_service)
        hooks = [_make_hook_def()]
        result = await service.create_convention(
            title="With Hooks",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
            hooks=hooks,
        )
        assert result.hooks == hooks
        feature_service.create_table.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_convention_without_hooks_skips_feature_tables(self):
        feature_service = AsyncMock()

        service = _make_service(feature_service=feature_service)
        await service.create_convention(
            title="No Hooks",
            version="1.0.0",
            schema=_make_field_defs(),
            file_requirements=_make_file_reqs(),
        )
        feature_service.create_table.assert_not_called()


class TestConventionServiceGet:
    @pytest.mark.asyncio
    async def test_get_existing(self):
        conv = Convention(
            srn=_make_conv_srn(),
            title="Test",
            schema_srn=_make_schema_srn(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        conv_repo = AsyncMock()
        conv_repo.get.return_value = conv

        service = _make_service(conv_repo=conv_repo)
        result = await service.get_convention(conv.srn)
        assert result == conv

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = None

        service = _make_service(conv_repo=conv_repo)
        with pytest.raises(NotFoundError):
            await service.get_convention(_make_conv_srn())


class TestConventionServiceList:
    @pytest.mark.asyncio
    async def test_list_conventions(self):
        conv = Convention(
            srn=_make_conv_srn(),
            title="Test",
            schema_srn=_make_schema_srn(),
            file_requirements=_make_file_reqs(),
            created_at=datetime.now(UTC),
        )
        conv_repo = AsyncMock()
        conv_repo.list.return_value = [conv]

        service = _make_service(conv_repo=conv_repo)
        result = await service.list_conventions()
        assert len(result) == 1
