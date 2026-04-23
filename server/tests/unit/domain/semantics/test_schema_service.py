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
from osa.domain.shared.error import ConflictError, NotFoundError, ValidationError
from osa.domain.shared.model.srn import Domain, OntologySRN, SchemaId, SchemaIdentifier


def _make_schema_id(id: str = "test-schema", version: str = "1.0.0") -> SchemaId:
    return SchemaId.parse(f"{id}@{version}")


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
        schema_repo.get.return_value = None
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_schema(
            id=SchemaIdentifier("simple-schema"),
            title="Simple Schema",
            version="1.0.0",
            fields=[_make_text_field()],
        )
        assert result.title == "Simple Schema"
        assert result.id.id.root == "simple-schema"
        schema_repo.save.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schema_with_valid_ontology_ref(self):
        schema_repo = AsyncMock()
        schema_repo.get.return_value = None
        ontology_repo = AsyncMock()
        ontology_repo.exists.return_value = True

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_schema(
            id=SchemaIdentifier("with-ontology"),
            title="With Ontology",
            version="1.0.0",
            fields=[_make_text_field(), _make_term_field()],
        )
        assert len(result.fields) == 2
        ontology_repo.exists.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_schema_rejects_invalid_ontology_ref(self):
        schema_repo = AsyncMock()
        schema_repo.get.return_value = None
        ontology_repo = AsyncMock()
        ontology_repo.exists.return_value = False

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        with pytest.raises(ValidationError, match="Ontology.*not found"):
            await service.create_schema(
                id=SchemaIdentifier("bad-ref"),
                title="Bad Ref",
                version="1.0.0",
                fields=[_make_term_field()],
            )

    @pytest.mark.asyncio
    async def test_create_schema_uses_supplied_id(self):
        schema_repo = AsyncMock()
        schema_repo.get.return_value = None
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        result = await service.create_schema(
            id=SchemaIdentifier("pdb-structure"),
            title="PDB Structures",
            version="1.0.0",
            fields=[_make_text_field()],
        )
        assert str(result.id) == "pdb-structure@1.0.0"

    @pytest.mark.asyncio
    async def test_duplicate_id_version_raises_conflict(self):
        schema_repo = AsyncMock()
        existing_schema = Schema(
            id=SchemaId.parse("dup@1.0.0"),
            title="Existing",
            fields=[_make_text_field()],
            created_at=datetime.now(UTC),
        )
        schema_repo.get.return_value = existing_schema
        ontology_repo = AsyncMock()

        service = SchemaService(
            schema_repo=schema_repo,
            ontology_repo=ontology_repo,
            node_domain=Domain("localhost"),
        )
        with pytest.raises(ConflictError) as exc:
            await service.create_schema(
                id=SchemaIdentifier("dup"),
                title="Dup",
                version="1.0.0",
                fields=[_make_text_field()],
            )
        assert exc.value.code == "schema_already_exists"
        schema_repo.save.assert_not_called()


class TestSchemaIdentifierValidation:
    def test_rejects_leading_digit(self):
        with pytest.raises(ValueError, match="invalid schema id"):
            SchemaIdentifier("3d-scan")

    def test_rejects_uppercase(self):
        with pytest.raises(ValueError, match="invalid schema id"):
            SchemaIdentifier("PDBStructure")

    def test_rejects_too_short(self):
        with pytest.raises(ValueError, match="invalid schema id"):
            SchemaIdentifier("ab")

    def test_rejects_underscore(self):
        with pytest.raises(ValueError, match="invalid schema id"):
            SchemaIdentifier("pdb_structure")

    def test_accepts_hyphens_and_digits(self):
        assert SchemaIdentifier("pdb-v2").root == "pdb-v2"


class TestSchemaServiceGet:
    @pytest.mark.asyncio
    async def test_get_existing(self):
        schema = Schema(
            id=_make_schema_id(),
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
        result = await service.get_schema(schema.id)
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
            await service.get_schema(_make_schema_id())


class TestSchemaServiceList:
    @pytest.mark.asyncio
    async def test_list_schemas(self):
        schema = Schema(
            id=_make_schema_id(),
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
