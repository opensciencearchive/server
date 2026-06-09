"""T064 — Schema.__init__ rejects reserved schema IDs (records, datasets)."""

from datetime import UTC, datetime

import pytest

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ReservedNameError
from osa.domain.shared.model.srn import SchemaId


def _text_field(name: str = "title") -> FieldDefinition:
    return FieldDefinition(
        name=name, type=FieldType.TEXT, required=True, cardinality=Cardinality.EXACTLY_ONE
    )


@pytest.mark.parametrize("reserved", ["records", "datasets"])
def test_schema_rejects_reserved_id(reserved: str) -> None:
    with pytest.raises(ReservedNameError) as exc:
        Schema(
            id=SchemaId.parse(f"{reserved}@1.0.0"),
            title="x",
            fields=[_text_field()],
            created_at=datetime.now(UTC),
        )
    assert exc.value.code == "reserved_name"
    assert exc.value.kind == "schema"
    assert exc.value.name == reserved


def test_schema_allows_non_reserved_id() -> None:
    schema = Schema(
        id=SchemaId.parse("compound@1.0.0"),
        title="Compound",
        fields=[_text_field()],
        created_at=datetime.now(UTC),
    )
    assert schema.id.id.root == "compound"
