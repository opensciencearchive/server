"""Unit tests for SchemaService."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import (
    Cardinality,
    FieldDefinition,
    FieldType,
    TermConstraints,
)
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import Domain, OntologySRN, SchemaSRN


def _make_schema_srn(id: str = "test-schema", version: str = "1.0.0") -> SchemaSRN:
    return SchemaSRN.parse(f"urn:osa:localhost:schema:{id}@{version}")


def _make_ontology_srn(id: str = "sex", version: str = "1.0.0") -> OntologySRN:
    return OntologySRN.parse(f"urn:osa:localhost:onto:{id}@{version}")


def _make_text_field(name: str = "title") -> FieldDefinition:
    return FieldDefinition(
        name=name,
        type=FieldType.TEXT,
        required=True,
        cardinality=Cardinality.EXACTLY_ONE,
    )


def _make_term_field(name: str = "sex", onto_srn: OntologySRN | None = None) -> FieldDefinition:
    return FieldDefinition(
        name=name,
        type=FieldType.TERM,
        required=True,
        cardinality=Cardinality.EXACTLY_ONE,
        constraints=TermConstraints(ontology_srn=onto_srn or _make_ontology_srn()),
    )


class TestSchemaServiceCreate:
    @pytest.mark.asyncio
    async def test_create_schema_without_ontology_refs(self):
        schema_repo = AsyncMock()
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_schema(
            title="Simple Schema",
            version="1.0.0",
            fields=[_make_text_field()],
        )
        assert result.title == "Simple Schema"
        schema_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schema_with_valid_ontology_ref(self):
        schema_repo = AsyncMock()
        ontology_repo = AsyncMock()
        ontology_repo.exists.return_value = True

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_schema(
            title="With Ontology",
            version="1.0.0",
            fields=[_make_text_field(), _make_term_field()],
        )
        assert len(result.fields) == 2
        ontology_repo.exists.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schema_rejects_invalid_ontology_ref(self):
        schema_repo = AsyncMock()
        ontology_repo = AsyncMock()
        ontology_repo.exists.return_value = False

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        with pytest.raises(ValidationError, match="Ontology.*not found"):
            await service.create_schema(
                title="Bad Ref",
                version="1.0.0",
                fields=[_make_term_field()],
            )

    @pytest.mark.asyncio
    async def test_create_schema_generates_srn(self):
        schema_repo = AsyncMock()
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_schema(
            title="Test",
            version="1.0.0",
            fields=[_make_text_field()],
        )
        assert str(result.srn).startswith("urn:osa:localhost:schema:")
        assert str(result.srn).endswith("@1.0.0")


class TestSchemaServiceGet:
    @pytest.mark.asyncio
    async def test_get_existing(self):
        schema = Schema(
            srn=_make_schema_srn(),
            title="Test",
            fields=[_make_text_field()],
            created_at=datetime.now(UTC),
        )
        schema_repo = AsyncMock()
        schema_repo.get.return_value = schema
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.get_schema(schema.srn)
        assert result == schema

    @pytest.mark.asyncio
    async def test_get_nonexistent_raises(self):
        schema_repo = AsyncMock()
        schema_repo.get.return_value = None
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        with pytest.raises(NotFoundError):
            await service.get_schema(_make_schema_srn())


class TestSchemaServiceList:
    @pytest.mark.asyncio
    async def test_list_schemas(self):
        schema = Schema(
            srn=_make_schema_srn(),
            title="Test",
            fields=[_make_text_field()],
            created_at=datetime.now(UTC),
        )
        schema_repo = AsyncMock()
        schema_repo.list.return_value = [schema]
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.list_schemas()
        assert len(result) == 1
