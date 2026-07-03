"""SkillRenderer reference-doc exact-output tests (#151, US1).

The reference doc is the markdown representation of the schema resource:
records field table (implicit columns first), one subsection per feature
table, join keys + provenance chain, mechanical examples templated with
actual field/feature names (sampled value when available), and author
worked examples rendered verbatim (FR-018).
"""

from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    IMPLICIT_RECORD_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.model.skill import AuthorDocs, ExampleDoc, SampleValue
from osa.domain.data.service.skill_renderer import SkillRenderer
from osa.domain.semantics.model.value import FieldType

BASE = "https://archive.university.edu"


def _fields() -> list[FieldSpec]:
    return [
        FieldSpec(
            name="yield_strength",
            type=FieldType.NUMBER,
            description="0.2% offset yield strength",
            unit="MPa",
            examples=["512"],
        ),
        FieldSpec(
            name="tissue",
            type=FieldType.TERM,
            ontology_id="uberon",
            ontology_version="2025-01-15",
        ),
    ]


def _manifest(*, with_feature: bool = True) -> SchemaManifest:
    resources = [
        TableResource(
            name="records",
            kind=TableKind.RECORDS,
            columns=[
                *IMPLICIT_RECORD_COLUMN_SPECS,
                ColumnSpec(name="yield_strength", type=FieldType.NUMBER),
                ColumnSpec(name="tissue", type=FieldType.TERM),
            ],
            row_count=12480,
            formats=["", "csv", "csv.gz"],
        )
    ]
    if with_feature:
        resources.append(
            TableResource(
                name="ductility",
                kind=TableKind.FEATURE,
                columns=[
                    *IMPLICIT_FEATURE_COLUMN_SPECS,
                    ColumnSpec(
                        name="transition_temp",
                        type=FieldType.NUMBER,
                        format="float",
                        unit="°C",
                        description="Ductile-brittle transition",
                    ),
                ],
                row_count=9312,
                formats=["", "csv", "csv.gz"],
            )
        )
    return SchemaManifest(
        id="alloy-tests",
        version="2.1.0",
        srn="urn:osa:archive.university.edu:schema:alloy-tests@2.1.0",
        title="Alloy Ductility Tests",
        fields=_fields(),
        table_resources=resources,
    )


def _docs() -> AuthorDocs:
    return AuthorDocs(
        purpose="Test-campaign results for structural alloys.",
        example_questions=["q1?", "q2?", "q3?"],
        examples=[
            ExampleDoc(
                question="Which alloys stay ductile below -40C?",
                query='POST /api/v1/data/alloy-tests/records {"filter": {"kind": "predicate", "field": "features.ductility.transition_temp", "op": "lt", "value": -40}}',
                interpretation="Rows are individual test coupons.",
            )
        ],
    )


def _render(**overrides: object) -> str:
    kwargs: dict = {
        "manifest": _manifest(),
        "docs": _docs(),
        "base_url": BASE,
        "sample_field": "yield_strength",
        "sample": SampleValue(value=512),
    }
    kwargs.update(overrides)
    return SkillRenderer().render_reference(**kwargs)


class TestReferenceHeader:
    def test_header_line(self) -> None:
        assert _render().startswith("# Alloy Ductility Tests (alloy-tests@2.1.0)\n")

    def test_header_falls_back_to_id_without_title(self) -> None:
        manifest = _manifest()
        manifest = manifest.model_copy(update={"title": None})
        out = _render(manifest=manifest)
        assert out.startswith("# alloy-tests (alloy-tests@2.1.0)\n")

    def test_purpose_paragraph_when_docs_exist(self) -> None:
        assert "\nTest-campaign results for structural alloys.\n" in _render()


class TestRecordsTable:
    def test_implicit_columns_documented_first(self) -> None:
        out = _render()
        table_start = out.index("## Records table")
        rows = [line for line in out[table_start:].split("\n") if line.startswith("| ")]
        # header + separator are not "| " prefixed with names — collect first cells
        first_cells = [r.split("|")[1].strip() for r in rows]
        implicit = ["id", "srn", "schema_id", "version", "created_at"]
        names = [c for c in first_cells if c not in ("Column", "---")]
        assert names[:5] == implicit

    def test_field_row_carries_unit_description_examples(self) -> None:
        assert (
            "| yield_strength | number | MPa | 0.2% offset yield strength |  | 512 |" in _render()
        )

    def test_term_field_carries_ontology_ref(self) -> None:
        assert "| tissue | term |  |  | uberon@2025-01-15 |  |" in _render()


class TestFeatureTables:
    def test_feature_subsection_with_column_metadata(self) -> None:
        out = _render()
        assert "## Feature tables" in out
        assert "### ductility" in out
        assert "| transition_temp | number | float | °C | Ductile-brittle transition |" in out

    def test_implicit_feature_columns_listed(self) -> None:
        out = _render()
        feature_start = out.index("### ductility")
        assert "| record_srn | text |" in out[feature_start:]


class TestJoinKeysAndProvenance:
    def test_join_keys(self) -> None:
        out = _render()
        assert "## Join keys & provenance" in out
        assert "`record_srn`" in out
        assert "`records.srn`" in out
        assert "`records.id`" in out

    def test_provenance_chain(self) -> None:
        out = _render()
        assert "`run_id`" in out
        assert "hook_run" in out
        assert "hook_release" in out
        assert "source_ref" in out


class TestMechanicalExamples:
    def test_bulk_dump_leads(self) -> None:
        out = _render()
        examples = out[out.index("## Examples") :]
        assert examples.index("records.csv.gz") < examples.index("FilterExpr")
        assert f"GET {BASE}/api/v1/data/alloy-tests/records.csv.gz" in examples

    def test_filter_example_uses_sampled_value_and_actual_field(self) -> None:
        out = _render()
        assert (
            '{"filter": {"kind": "predicate", "field": "metadata.yield_strength", '
            '"op": "eq", "value": 512}}'
        ) in out

    def test_filter_example_placeholder_when_no_sample(self) -> None:
        out = _render(sample=None)
        assert '"value": "REPLACE_WITH_A_REAL_VALUE"' in out

    def test_join_recipe_spells_out_columns(self) -> None:
        out = _render()
        assert f"GET {BASE}/api/v1/data/alloy-tests/ductility.csv.gz" in out
        assert "`ductility.record_srn`" in out

    def test_single_record_fetch(self) -> None:
        assert f"GET {BASE}/api/v1/data/records/<id>" in _render()


class TestWorkedExamples:
    def test_rendered_verbatim(self) -> None:
        out = _render()
        assert "## Worked examples" in out
        assert "### Which alloys stay ductile below -40C?" in out
        # The query string appears byte-for-byte (FR-018) — never rewritten.
        assert (
            'POST /api/v1/data/alloy-tests/records {"filter": {"kind": "predicate", '
            '"field": "features.ductility.transition_temp", "op": "lt", "value": -40}}'
        ) in out
        assert "Rows are individual test coupons." in out
