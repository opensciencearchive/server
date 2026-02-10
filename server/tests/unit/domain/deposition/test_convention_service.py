"""Unit tests for ConventionService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import ConventionSRN, Domain, SchemaSRN


def _make_conv_srn(id: str = "test-conv", version: str = "1.0.0") -> ConventionSRN:
    return ConventionSRN.parse(f"urn:osa:localhost:conv:{id}@{version}")


def _make_schema_srn(id: str = "test-schema", version: str = "1.0.0") -> SchemaSRN:
    return SchemaSRN.parse(f"urn:osa:localhost:schema:{id}@{version}")


def _make_file_reqs() -> FileRequirements:
    return FileRequirements(
        accepted_types=[".csv"],
        min_count=1,
        max_count=3,
        max_file_size=1_000_000,
    )


class TestConventionServiceCreate:
    @pytest.mark.asyncio
    async def test_create_convention_with_valid_schema(self):
        conv_repo = AsyncMock()
        schema_reader = AsyncMock()
        schema_reader.schema_exists.return_value = True

        service = ConventionService(
            convention_repo=conv_repo,
            schema_reader=schema_reader,
            node_domain=Domain("localhost"),
        )
        result = await service.create_convention(
            title="Test Convention",
            version="1.0.0",
            schema_srn=_make_schema_srn(),
            file_requirements=_make_file_reqs(),
        )
        assert result.title == "Test Convention"
        conv_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_convention_rejects_invalid_schema(self):
        conv_repo = AsyncMock()
        schema_reader = AsyncMock()
        schema_reader.schema_exists.return_value = False

        service = ConventionService(
            convention_repo=conv_repo,
            schema_reader=schema_reader,
            node_domain=Domain("localhost"),
        )
        with pytest.raises(ValidationError, match="Schema.*not found"):
            await service.create_convention(
                title="Bad",
                version="1.0.0",
                schema_srn=_make_schema_srn(),
                file_requirements=_make_file_reqs(),
            )

    @pytest.mark.asyncio
    async def test_create_convention_generates_srn(self):
        conv_repo = AsyncMock()
        schema_reader = AsyncMock()
        schema_reader.schema_exists.return_value = True

        service = ConventionService(
            convention_repo=conv_repo,
            schema_reader=schema_reader,
            node_domain=Domain("localhost"),
        )
        result = await service.create_convention(
            title="Test",
            version="1.0.0",
            schema_srn=_make_schema_srn(),
            file_requirements=_make_file_reqs(),
        )
        assert str(result.srn).startswith("urn:osa:localhost:conv:")


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
        schema_reader = AsyncMock()

        service = ConventionService(
            convention_repo=conv_repo,
            schema_reader=schema_reader,
            node_domain=Domain("localhost"),
        )
        result = await service.get_convention(conv.srn)
        assert result == conv

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises(self):
        conv_repo = AsyncMock()
        conv_repo.get.return_value = None
        schema_reader = AsyncMock()

        service = ConventionService(
            convention_repo=conv_repo,
            schema_reader=schema_reader,
            node_domain=Domain("localhost"),
        )
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
        schema_reader = AsyncMock()

        service = ConventionService(
            convention_repo=conv_repo,
            schema_reader=schema_reader,
            node_domain=Domain("localhost"),
        )
        result = await service.list_conventions()
        assert len(result) == 1
