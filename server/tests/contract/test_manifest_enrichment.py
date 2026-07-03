"""DB-free contract tests for the enriched schema manifest (#151, US3).

The manifest DTOs gain optional metadata (`title`, field `description`/`unit`/
`examples`, column `format`/`description`/`unit`) — strictly additive (FR-019).
Absent attributes must be omitted from serialized output so a field carrying
none of the new attributes serializes exactly as before (AS-2 regression).

DB-backed round-trip behaviour lives in
``tests/integration/test_manifest_enrichment_postgres.py``.
"""

import os

from osa.domain.data.model.manifest import ColumnSpec, FieldSpec, SchemaManifest
from osa.domain.semantics.model.value import FieldType

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-contract-tests-minimum-32-chars")


class TestFieldSpecEnrichment:
    def test_accepts_description_unit_examples(self):
        spec = FieldSpec(
            name="yield_strength",
            type=FieldType.NUMBER,
            description="0.2% offset yield strength",
            unit="MPa",
            examples=["512"],
        )
        assert spec.description == "0.2% offset yield strength"
        assert spec.unit == "MPa"
        assert spec.examples == ["512"]

    def test_bare_field_serializes_exactly_as_today(self):
        # AS-2 regression: no new keys appear on a field with no new attributes.
        spec = FieldSpec(name="alloy", type=FieldType.TEXT)
        assert spec.model_dump(exclude_none=True) == {"name": "alloy", "type": FieldType.TEXT}

    def test_term_field_keeps_ontology_ref_without_new_keys(self):
        spec = FieldSpec(
            name="tissue",
            type=FieldType.TERM,
            ontology_id="uberon",
            ontology_version="2025-01-15",
        )
        assert spec.model_dump(exclude_none=True) == {
            "name": "tissue",
            "type": FieldType.TERM,
            "ontology_id": "uberon",
            "ontology_version": "2025-01-15",
        }


class TestColumnSpecEnrichment:
    def test_accepts_format_description_unit(self):
        spec = ColumnSpec(
            name="transition_temp",
            type=FieldType.NUMBER,
            format="float",
            description="Ductile-brittle transition",
            unit="°C",
        )
        assert spec.format == "float"
        assert spec.description == "Ductile-brittle transition"
        assert spec.unit == "°C"

    def test_bare_column_serializes_exactly_as_today(self):
        spec = ColumnSpec(name="score", type=FieldType.NUMBER)
        assert spec.model_dump(exclude_none=True) == {"name": "score", "type": FieldType.NUMBER}


class TestSchemaManifestEnrichment:
    def test_carries_title(self):
        manifest = SchemaManifest(
            id="alloy-tests",
            version="2.1.0",
            srn="urn:osa:localhost:schema:alloy-tests@2.1.0",
            title="Alloy Ductility Tests",
            fields=[],
            table_resources=[],
        )
        assert manifest.title == "Alloy Ductility Tests"

    def test_title_is_required(self):
        # schemas.title is NOT NULL — a title-less manifest is unrepresentable.
        import pytest
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            SchemaManifest(
                id="alloy-tests",
                version="2.1.0",
                srn="urn:osa:localhost:schema:alloy-tests@2.1.0",
                fields=[],
                table_resources=[],
            )  # type: ignore[call-arg]


def test_manifest_route_omits_absent_attributes():
    """The manifest route must serialize with exclude_none so absent metadata
    never appears as ``null`` keys on the wire (FR-019/AS-2)."""
    from osa.application.api.rest.app import create_app

    app = create_app()
    route = next(
        r for r in app.routes if getattr(r, "operation_id", None) == "data_get_schema_manifest"
    )
    assert route.response_model is SchemaManifest
    assert route.response_model_exclude_none is True
