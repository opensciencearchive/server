"""Unit tests for Schema aggregate."""

from datetime import UTC, datetime

import pytest

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import (
    Cardinality,
    FieldDefinition,
    FieldType,
    NumberConstraints,
    TermConstraints,
    TextConstraints,
)
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import OntologySRN, SchemaSRN


def _make_srn(id: str = "test-schema", version: str = "1.0.0") -> SchemaSRN:
    return SchemaSRN.parse(f"urn:osa:localhost:schema:{id}@{version}")


def _make_text_field(name: str = "title", required: bool = True) -> FieldDefinition:
    return FieldDefinition(
        name=name,
        type=FieldType.TEXT,
        required=required,
        cardinality=Cardinality.EXACTLY_ONE,
    )


class TestSchemaCreation:
    def test_create_with_single_field(self):
        schema = Schema(
            srn=_make_srn(),
            title="Test Schema",
            fields=[_make_text_field()],
            created_at=datetime.now(UTC),
        )
        assert schema.title == "Test Schema"
        assert len(schema.fields) == 1

    def test_create_with_multiple_fields(self):
        schema = Schema(
            srn=_make_srn(),
            title="scRNA-seq",
            fields=[
                _make_text_field("title"),
                FieldDefinition(
                    name="sample_count",
                    type=FieldType.NUMBER,
                    required=True,
                    cardinality=Cardinality.EXACTLY_ONE,
                    constraints=NumberConstraints(integer_only=True, min_value=1),
                ),
            ],
            created_at=datetime.now(UTC),
        )
        assert len(schema.fields) == 2

    def test_create_with_ontology_reference(self):
        onto_srn = OntologySRN.parse("urn:osa:localhost:onto:sex@1.0.0")
        schema = Schema(
            srn=_make_srn(),
            title="With Ontology",
            fields=[
                FieldDefinition(
                    name="sex",
                    type=FieldType.TERM,
                    required=True,
                    cardinality=Cardinality.EXACTLY_ONE,
                    constraints=TermConstraints(ontology_srn=onto_srn),
                ),
            ],
            created_at=datetime.now(UTC),
        )
        assert schema.fields[0].constraints.ontology_srn == onto_srn

    def test_create_with_text_constraints(self):
        schema = Schema(
            srn=_make_srn(),
            title="Constrained",
            fields=[
                FieldDefinition(
                    name="title",
                    type=FieldType.TEXT,
                    required=True,
                    cardinality=Cardinality.EXACTLY_ONE,
                    constraints=TextConstraints(min_length=1, max_length=500),
                ),
            ],
            created_at=datetime.now(UTC),
        )
        assert schema.fields[0].constraints.max_length == 500


class TestSchemaInvariants:
    def test_rejects_empty_fields(self):
        with pytest.raises(ValidationError, match="at least one field"):
            Schema(
                srn=_make_srn(),
                title="Empty",
                fields=[],
                created_at=datetime.now(UTC),
            )

    def test_rejects_duplicate_field_names(self):
        with pytest.raises(ValidationError, match="Duplicate field names"):
            Schema(
                srn=_make_srn(),
                title="Duplicate",
                fields=[
                    _make_text_field("title"),
                    _make_text_field("title"),
                ],
                created_at=datetime.now(UTC),
            )
