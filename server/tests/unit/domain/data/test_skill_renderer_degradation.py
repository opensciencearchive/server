"""Renderer degradation tests (#151, US4) — degraded, never broken.

Empty catalog, schema without feature tables, orphan schema (no owning
convention → ``docs is None``), and empty-column sampling all render valid
documents (FR-021..023).
"""

from osa.domain.data.model.manifest import (
    IMPLICIT_RECORD_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.model.skill import DatasetEntry, NodeIdentity
from osa.domain.data.service.skill_renderer import SkillRenderer

BASE = "http://localhost:8000"


def _node() -> NodeIdentity:
    return NodeIdentity(
        name="Open Science Archive",
        domain="localhost",
        description="An open platform.",
        osa_version="0.0.5",
    )


def _manifest_no_features() -> SchemaManifest:
    from osa.domain.semantics.model.value import FieldType

    return SchemaManifest(
        id="orphan",
        version="1.0.0",
        srn="urn:osa:localhost:schema:orphan@1.0.0",
        title="Orphan Records",
        fields=[FieldSpec(name="species", type=FieldType.TEXT)],
        table_resources=[
            TableResource(
                name="records",
                kind=TableKind.RECORDS,
                columns=[
                    *IMPLICIT_RECORD_COLUMN_SPECS,
                    ColumnSpec(name="species", type=FieldType.TEXT),
                ],
                row_count=0,
                formats=["", "csv", "csv.gz"],
            )
        ],
    )


class TestEmptyNodeSkill:
    def test_zero_schemas_renders_valid_document_with_note(self) -> None:
        out = SkillRenderer().render_skill(node=_node(), base_url=BASE, datasets=[], docs=[])
        assert out.startswith("---\nname: osa-data-localhost\n")
        assert "## Datasets" in out
        assert "No datasets yet." in out
        # No dataset rows
        assert "api/v1/data/" not in out.split("## Access")[0].split("## Datasets")[1]


class TestSchemaWithoutFeatureTables:
    def test_feature_section_omitted_entirely(self) -> None:
        out = SkillRenderer().render_reference(
            manifest=_manifest_no_features(),
            docs=None,
            base_url=BASE,
            sample_field="species",
            sample=None,
        )
        assert "## Feature tables" not in out
        assert "## Records table" in out


class TestOrphanSchema:
    def test_no_docs_note_and_sections_omitted(self) -> None:
        out = SkillRenderer().render_reference(
            manifest=_manifest_no_features(),
            docs=None,
            base_url=BASE,
            sample_field="species",
            sample=None,
        )
        assert "> No author documentation exists for this dataset." in out
        assert "## Worked examples" not in out
        assert "## When not to use" not in out

    def test_orphan_schema_still_in_datasets_table(self) -> None:
        ds = DatasetEntry(schema_ref="orphan@1.0.0", title="Orphan Records", row_count=0)
        out = SkillRenderer().render_skill(node=_node(), base_url=BASE, datasets=[ds], docs=[])
        assert "| orphan@1.0.0 |" in out


class TestEmptyColumnSampling:
    def test_placeholder_when_sample_is_none(self) -> None:
        out = SkillRenderer().render_reference(
            manifest=_manifest_no_features(),
            docs=None,
            base_url=BASE,
            sample_field="species",
            sample=None,
        )
        assert '"value": "REPLACE_WITH_A_REAL_VALUE"' in out

    def test_examples_still_render_without_any_field(self) -> None:
        manifest = _manifest_no_features().model_copy(update={"fields": []})
        out = SkillRenderer().render_reference(
            manifest=manifest, docs=None, base_url=BASE, sample_field=None, sample=None
        )
        assert "## Examples" in out
        assert "records.csv.gz" in out
