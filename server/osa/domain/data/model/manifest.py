"""Schema manifest response envelope (FR-002, research §9).

A stable, machine-readable cross-schema shape suitable for SDK code
generation and agent affordance: typed field definitions plus the set of
addressable table resources (the ``records`` table and every feature table),
each with its column schema, row count, and the URL-exposed format suffixes.
"""

from __future__ import annotations

from pydantic import BaseModel

from osa.domain.data.model.query_plan import TableKind
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.model.srn import SchemaId


class FieldSpec(BaseModel):
    """A schema-declared metadata field."""

    name: str
    type: FieldType
    ontology_id: str | None = None  # required iff type == TERM
    ontology_version: str | None = None  # required iff type == TERM


class ColumnSpec(BaseModel):
    """A physical column on an addressable table resource."""

    name: str
    type: FieldType


# Implicit columns present on every records-table response, in wire order,
# ahead of the schema's declared metadata fields (data-model.md §ColumnSpec).
IMPLICIT_RECORD_COLUMN_SPECS: tuple[ColumnSpec, ...] = (
    ColumnSpec(name="id", type=FieldType.TEXT),
    ColumnSpec(name="srn", type=FieldType.TEXT),
    ColumnSpec(name="schema_id", type=FieldType.TEXT),
    ColumnSpec(name="version", type=FieldType.NUMBER),
    ColumnSpec(name="created_at", type=FieldType.DATE),
)

# Implicit columns present on every feature-table response, in wire order, ahead
# of the hook's declared output columns. Feature rows have no SRN in v1 (data-
# model.md §RecordSummary); ``record_srn`` is the join key back to ``records``.
IMPLICIT_FEATURE_COLUMN_SPECS: tuple[ColumnSpec, ...] = (
    ColumnSpec(name="id", type=FieldType.NUMBER),
    ColumnSpec(name="record_srn", type=FieldType.TEXT),
    ColumnSpec(name="created_at", type=FieldType.DATE),
)


class TableResource(BaseModel):
    """One addressable table under a schema: the records table or a feature table."""

    name: str  # "records" or the feature table name
    kind: TableKind
    columns: list[ColumnSpec]
    row_count: int
    formats: list[str]  # URL suffixes, e.g. ["", "csv", "csv.gz"]


class SchemaManifest(BaseModel):
    """Full machine-readable manifest for a single schema version."""

    id: str  # short schema id, e.g. "compound"
    version: str  # SemVer, e.g. "1.0.0"
    srn: str  # full schema SRN
    fields: list[FieldSpec]
    table_resources: list[TableResource]


class ResolvedTable(BaseModel):
    """A table resolved for reading: the owning schema plus its column schema.

    Produced by ``DataCatalogService.resolve_table`` — the single owner of
    manifest-structure knowledge (the records resource is named ``records``;
    feature resources carry kind ``FEATURE``). Route code consumes this
    instead of picking through ``table_resources`` itself.
    """

    schema_id: SchemaId
    columns: list[ColumnSpec]
