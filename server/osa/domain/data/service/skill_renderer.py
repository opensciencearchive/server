"""SkillRenderer — pure markdown rendering for the skill surface (#151).

String in/out, no I/O (research §3): the generator assembles catalog data,
author docs, and samples; this service turns them into the two documents —
``SKILL.md`` and the per-schema reference. Exact output is pinned by unit
tests, so every formatting decision lives here and nowhere else.
"""

from __future__ import annotations

import json
import re
from urllib.parse import quote

from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.model.skill import AuthorDocs, DatasetEntry, NodeIdentity, SampleValue
from osa.domain.shared.service import Service

# A clearly-labeled stand-in used when no real value could be sampled
# (empty column/table — FR-021/022); the example stays syntactically valid.
PLACEHOLDER_VALUE = "REPLACE_WITH_A_REAL_VALUE"

# Auto columns on every feature table — excluded when picking a data column to
# template a feature-filter example on.
_IMPLICIT_FEATURE_NAMES = {c.name for c in IMPLICIT_FEATURE_COLUMN_SPECS}

# Fixed documentation for the implicit records-table columns (wire order).
_IMPLICIT_RECORD_DOCS: list[tuple[str, str, str]] = [
    ("id", "text", "Record identifier (node-assigned, stable)"),
    ("srn", "text", "Full record SRN — join key for feature tables"),
    ("schema_id", "text", "Owning schema id"),
    ("version", "number", "Record version"),
    ("created_at", "date", "Publication timestamp"),
]

_IMPLICIT_RECORD_NAMES = {name for name, _, _ in _IMPLICIT_RECORD_DOCS}


def sanitize_skill_name(domain: str) -> str:
    """``osa-data-<domain>`` with every char outside [a-z0-9] mapped to ``-``,
    runs collapsed (FR-006; research §8)."""
    slug = re.sub(r"-+", "-", re.sub(r"[^a-z0-9]", "-", domain.lower())).strip("-")
    return f"osa-data-{slug}"


def _one_line(text: str) -> str:
    return " ".join(text.split())


def _reference_path(schema_ref: str) -> str:
    # `@` stays literal: it is valid in a path segment and the versioned ref
    # must remain copy-pasteable.
    return quote(f"api/v1/data/{schema_ref}.md", safe="/@")


class SkillRenderer(Service):
    """Pure markdown rendering — no ports, no I/O."""

    # ------------------------------------------------------------------ #
    # SKILL.md
    # ------------------------------------------------------------------ #

    def render_skill(
        self,
        *,
        node: NodeIdentity,
        base_url: str,
        datasets: list[DatasetEntry],
        docs: list[AuthorDocs],
    ) -> str:
        lines: list[str] = []
        lines.append("---")
        lines.append(f"name: {sanitize_skill_name(node.domain)}")
        lines.append(f"description: {self._skill_description(node, docs)}")
        lines.append("---")
        lines.append("")
        lines.append(f"# {node.name}")

        for d in docs:
            if d.purpose.strip():
                lines.append("")
                lines.append(_one_line(d.purpose))

        lines.append("")
        lines.append("## Datasets")
        lines.append("")
        if datasets:
            lines.append("| Schema | Title | Records | Reference |")
            lines.append("|---|---|---|---|")
            for ds in datasets:
                lines.append(
                    f"| {ds.schema_ref} | {ds.title} | {ds.row_count} "
                    f"| {_reference_path(ds.schema_ref)} |"
                )
        else:
            lines.append("No datasets yet.")

        coverage = [(ds, fc) for ds in datasets for fc in ds.features]
        if coverage:
            lines.append("")
            lines.append("Feature-table coverage (rows · records covered):")
            lines.append("")
            for ds, fc in coverage:
                lines.append(
                    f"- {ds.schema_ref} · {fc.name}: {fc.row_count} rows, "
                    f"{fc.records_covered}/{ds.row_count} records covered"
                )

        lines.append("")
        lines.append("## Access")
        lines.append("")
        # Start here: records hold identifiers + metadata; the science is in the
        # feature tables, joined back on record_srn. Say so before the endpoints,
        # so an agent doesn't download the id-only records dump expecting data.
        lines.append(
            "Records tables carry identifiers and metadata; the measured and derived "
            "science lives in each schema's feature tables (listed in its Reference doc). "
            "Join feature rows to records on `record_srn` = `records.srn`."
        )
        lines.append("")
        lines.append(f"- Bulk:   GET  {base_url}/api/v1/data/<schema>/records.csv.gz")
        lines.append(
            f"- Filter: POST {base_url}/api/v1/data/<schema>/records "
            f"(FilterExpr body — use for large tables)"
        )
        lines.append(f"- One:    GET  {base_url}/api/v1/data/records/<id>")

        when_not = [d.when_not_to_use for d in docs if d.when_not_to_use]
        if when_not:
            lines.append("")
            lines.append("## When not to use")
            for text in when_not:
                lines.append("")
                lines.append(_one_line(text))

        see_also = [entry for d in docs if d.see_also for entry in d.see_also]
        if see_also:
            lines.append("")
            lines.append("## See also")
            lines.append("")
            for entry in see_also:
                lines.append(f"- {entry}")

        return "\n".join(lines) + "\n"

    @staticmethod
    def _skill_description(node: NodeIdentity, docs: list[AuthorDocs]) -> str:
        questions: dict[str, None] = {}
        for d in docs:
            for q in d.trigger_questions():
                questions.setdefault(q)
        description = _one_line(node.description)
        if questions:
            description += " Use when answering questions like: " + " ".join(questions)
        return description

    # ------------------------------------------------------------------ #
    # Reference document
    # ------------------------------------------------------------------ #

    @staticmethod
    def filter_example_field(manifest: SchemaManifest) -> str | None:
        """The field templated into the FilterExpr example — the first declared
        metadata field (research §9). ``None`` when the schema has no fields."""
        return manifest.fields[0].name if manifest.fields else None

    def render_reference(
        self,
        *,
        manifest: SchemaManifest,
        docs: AuthorDocs | None,
        base_url: str,
        sample_field: str | None,
        sample: SampleValue | None,
    ) -> str:
        schema_ref = f"{manifest.id}@{manifest.version}"
        features = [t for t in manifest.table_resources if t.kind == TableKind.FEATURE]

        lines: list[str] = []
        lines.append(f"# {manifest.title} ({schema_ref})")

        if docs is not None:
            lines.append("")
            lines.append(_one_line(docs.purpose))
        else:
            lines.append("")
            lines.append("> No author documentation exists for this dataset.")

        lines.extend(self._records_table_section(manifest))

        if features:
            records = next(
                (t for t in manifest.table_resources if t.kind == TableKind.RECORDS), None
            )
            records_total = records.row_count if records is not None else 0
            lines.append("")
            lines.append("## Feature tables")
            for feature in features:
                lines.extend(self._feature_section(feature, records_total))

        lines.extend(self._join_provenance_section(base_url, features))
        lines.extend(
            self._mechanical_examples(
                manifest=manifest,
                base_url=base_url,
                features=features,
                sample_field=sample_field,
                sample=sample,
            )
        )

        if docs is not None:
            lines.extend(self._worked_examples(docs))
            if docs.when_not_to_use:
                lines.append("")
                lines.append("## When not to use")
                lines.append("")
                lines.append(_one_line(docs.when_not_to_use))
            if docs.see_also:
                lines.append("")
                lines.append("## See also")
                lines.append("")
                for entry in docs.see_also:
                    lines.append(f"- {entry}")

        return "\n".join(lines) + "\n"

    def _records_table_section(self, manifest: SchemaManifest) -> list[str]:
        lines = ["", "## Records table", ""]
        lines.append("| Column | Type | Unit | Description | Ontology | Examples |")
        lines.append("|---|---|---|---|---|---|")
        for name, type_, description in _IMPLICIT_RECORD_DOCS:
            lines.append(f"| {name} | {type_} |  | {description} |  |  |")
        for f in manifest.fields:
            if f.name in _IMPLICIT_RECORD_NAMES:
                continue
            ontology = (
                f"{f.ontology_id}@{f.ontology_version}"
                if f.ontology_id and f.ontology_version
                else (f.ontology_id or "")
            )
            examples = ", ".join(f.examples) if f.examples else ""
            lines.append(
                f"| {f.name} | {f.type.value} | {f.unit or ''} | {f.description or ''} "
                f"| {ontology} | {examples} |"
            )
        return lines

    @staticmethod
    def _feature_section(feature: TableResource, records_total: int) -> list[str]:
        lines = ["", f"### {feature.name}", ""]
        if feature.records_covered is not None:
            pct = (
                f" ({round(100 * feature.records_covered / records_total)}%)"
                if records_total
                else ""
            )
            lines.append(
                f"{feature.row_count} rows · covers {feature.records_covered} of "
                f"{records_total} records{pct}."
            )
            lines.append("")
        # The Filter ref column is the one datum needed to filter a column, sat
        # right next to the column: features.<hook>.<column> (the hook == the
        # feature-table name). Usable today on this feature's own table stream.
        lines.append("| Column | Type | Format | Unit | Description | Filter ref |")
        lines.append("|---|---|---|---|---|---|")
        for c in feature.columns:
            lines.append(
                f"| {c.name} | {c.type.value} | {c.format or ''} | {c.unit or ''} "
                f"| {c.description or ''} | features.{feature.name}.{c.name} |"
            )
        return lines

    @staticmethod
    def _join_provenance_section(base_url: str, features: list[TableResource]) -> list[str]:
        lines = ["", "## Join keys & provenance", ""]
        if features:
            lines.append(
                "- Feature rows join to records on `record_srn` = `records.srn`; "
                "single records also resolve by bare `records.id`."
            )
        else:
            lines.append("- Single records resolve by bare `records.id` or full `records.srn`.")
        lines.append(
            "- Provenance: every feature row's `run_id` resolves to a `hook_run`, "
            "which references the `hook_release` (image, digest, config, source_ref) "
            "that produced it."
        )
        return lines

    def _mechanical_examples(
        self,
        *,
        manifest: SchemaManifest,
        base_url: str,
        features: list[TableResource],
        sample_field: str | None,
        sample: SampleValue | None,
    ) -> list[str]:
        data_base = f"{base_url}/api/v1/data"
        # Fully-qualified <id>@<version>: the doc may have been requested at a
        # pinned version, and a bare id resolves to *latest* — the examples must
        # target exactly the schema version this document describes.
        schema_ref = f"{manifest.id}@{manifest.version}"
        lines = ["", "## Examples", ""]
        n = 1
        lines.append(f"{n}. Bulk dump (start here):")
        lines.append("")
        lines.append(f"   GET {data_base}/{schema_ref}/records.csv.gz")
        if sample_field is not None:
            n += 1
            value = sample.value if sample is not None else PLACEHOLDER_VALUE
            body = json.dumps(
                {
                    "filter": {
                        "kind": "predicate",
                        "field": f"metadata.{sample_field}",
                        "op": "eq",
                        "value": value,
                    }
                },
                ensure_ascii=False,
            )
            lines.append("")
            lines.append(f"{n}. Filtered query (FilterExpr POST — use for large tables):")
            lines.append("")
            lines.append(f"   POST {data_base}/{schema_ref}/records")
            lines.append(f"   {body}")
        if features:
            n += 1
            feature = features[0]
            lines.append("")
            lines.append(
                f"{n}. Record ↔ feature join: fetch the feature table and join its "
                f"`{feature.name}.record_srn` column to `records.srn`:"
            )
            lines.append("")
            lines.append(f"   GET {data_base}/{schema_ref}/{feature.name}.csv.gz")
            data_cols = [c for c in feature.columns if c.name not in _IMPLICIT_FEATURE_NAMES]
            if data_cols:
                n += 1
                col = data_cols[0].name
                fbody = json.dumps(
                    {
                        "filter": {
                            "kind": "predicate",
                            "field": f"features.{feature.name}.{col}",
                            "op": "eq",
                            "value": PLACEHOLDER_VALUE,
                        }
                    },
                    ensure_ascii=False,
                )
                lines.append("")
                lines.append(f"{n}. Filter a feature table by one of its columns:")
                lines.append("")
                lines.append(f"   POST {data_base}/{schema_ref}/{feature.name}")
                lines.append(f"   {fbody}")
        n += 1
        lines.append("")
        lines.append(f"{n}. Single record:")
        lines.append("")
        lines.append(f"   GET {data_base}/records/<id>")
        return lines

    @staticmethod
    def _worked_examples(docs: AuthorDocs) -> list[str]:
        if not docs.examples:
            return []
        lines = ["", "## Worked examples"]
        for example in docs.examples:
            lines.append("")
            lines.append(f"### {example.question}")
            lines.append("")
            lines.append("```")
            lines.append(example.query)  # verbatim — never rewritten (FR-018)
            lines.append("```")
            lines.append("")
            lines.append(example.interpretation)
        return lines
