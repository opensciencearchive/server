"""FilterPanelData.from_manifest — manifest → facet derivation (#162).

Pure projection of a ``SchemaManifest`` into typed facet controls: range for
number/date, select for boolean/term, text-contains for text/url. Facet
``field`` values are the dotted paths the FilterExpr grammar accepts, so a
consumer can emit a valid filter without further mapping.
"""

import pytest

from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.model.view import FacetKind, FilterPanelData
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import NotFoundError


def _manifest() -> SchemaManifest:
    return SchemaManifest(
        id="alloy-sample",
        version="1.0.0",
        srn="urn:osa:localhost:schema:alloy-sample@1.0.0",
        title="Alloy sample",
        fields=[
            FieldSpec(name="sample_id", type=FieldType.TEXT),
            FieldSpec(name="alloy_family", type=FieldType.TERM, ontology_id="osa:alloy"),
            FieldSpec(name="mass", type=FieldType.NUMBER, unit="g"),
            FieldSpec(name="collected_on", type=FieldType.DATE),
            FieldSpec(name="certified", type=FieldType.BOOLEAN),
            FieldSpec(name="datasheet", type=FieldType.URL),
        ],
        table_resources=[
            TableResource(
                name="records",
                kind=TableKind.RECORDS,
                columns=[],
                row_count=10,
                formats=[""],
            ),
            TableResource(
                name="tensile_test",
                kind=TableKind.FEATURE,
                columns=[
                    *IMPLICIT_FEATURE_COLUMN_SPECS,
                    ColumnSpec(name="stress", type=FieldType.NUMBER, unit="MPa"),
                    ColumnSpec(name="phase", type=FieldType.TEXT),
                ],
                row_count=100,
                formats=[""],
            ),
        ],
    )


class TestRecordsFacets:
    def test_metadata_fields_become_facets_with_dotted_refs(self):
        spec = FilterPanelData.from_manifest(_manifest(), "records")
        by_field = {f.field: f for f in spec.facets}
        assert by_field["metadata.mass"].kind == FacetKind.RANGE
        assert by_field["metadata.collected_on"].kind == FacetKind.RANGE
        assert by_field["metadata.certified"].kind == FacetKind.SELECT
        assert by_field["metadata.alloy_family"].kind == FacetKind.SELECT
        assert by_field["metadata.sample_id"].kind == FacetKind.TEXT
        assert by_field["metadata.datasheet"].kind == FacetKind.TEXT

    def test_unit_carried_through(self):
        spec = FilterPanelData.from_manifest(_manifest(), "records")
        mass = next(f for f in spec.facets if f.field == "metadata.mass")
        assert mass.unit == "g"


class TestFeatureFacets:
    def test_declared_columns_only_no_implicit(self):
        spec = FilterPanelData.from_manifest(_manifest(), "tensile_test")
        fields = [f.field for f in spec.facets]
        assert "features.tensile_test.stress" in fields
        assert "features.tensile_test.phase" in fields
        # Implicit columns are not FilterExpr-addressable — never facet them.
        assert not any(f.endswith(".id") for f in fields)
        assert not any("record_srn" in f for f in fields)
        assert not any("created_at" in f for f in fields)


class TestUnknownTable:
    def test_unknown_table_raises_not_found(self):
        with pytest.raises(NotFoundError):
            FilterPanelData.from_manifest(_manifest(), "nope")
