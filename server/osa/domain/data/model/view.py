"""View models — interactive-consumption projections over published data (#162).

Payload shapes for view-style reads: a bounded, JSON-safe table page with its
paging state; the dataset list with row counts; a record with its joinable
feature tables; facet controls derived from a manifest; a bounded column
sample. Protocol adapters (the MCP surface today, a first-party canvas later)
deliver these verbatim — all shaping happens here and in ``DataViewService``.

JSON-safe rows follow the established posture of :meth:`RecordSummary.flatten`
and the JSON serializer: non-primitive values (datetimes, Decimals) are
rendered to their string form, never left as Python objects.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field, JsonValue

from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    ColumnSpec,
    SchemaManifest,
)
from osa.domain.data.model.query_plan import SortSpec, TableKind
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import FeatureName

# The records table plus feature tables share the PG-identifier shape; the
# literal name "records" selects the records table (same convention as the
# ``/data/{schema}/{table}`` URL space).
RECORDS_TABLE = "records"


class TableQuery(BaseModel):
    """The query context a consumer needs to re-issue or continue a table read."""

    schema: str
    table: str
    filter: FilterExpr | None = None
    sort: list[SortSpec] = Field(default_factory=list)
    limit: int


class TablePage(BaseModel):
    """One bounded page of a table read, JSON-safe, plus paging state.

    ``truncated`` means more rows exist beyond this page — consumers must
    surface that rather than silently presenting a partial picture.
    """

    query: TableQuery
    columns: list[ColumnSpec]
    rows: list[dict[str, JsonValue]]
    next_cursor: str | None = None
    truncated: bool = False


class DatasetSummary(BaseModel):
    """One published schema in the dataset list."""

    id: str
    version: str
    srn: str
    title: str
    record_count: int
    feature_tables: list[FeatureName]


class DatasetList(BaseModel):
    node_domain: str
    datasets: list[DatasetSummary]


class RecordDetailData(BaseModel):
    """A record plus the feature tables a detail view can join on ``record_srn``."""

    record: RecordSummary
    schema: str
    feature_tables: list[FeatureName]


class ColumnSample(BaseModel):
    """Bounded, deduped non-null scalar values of one column (facet options)."""

    column: str
    values: list[str | int | float | bool]
    truncated: bool


# ── Filter-panel facets ──


class FacetKind(StrEnum):
    RANGE = "range"  # number / date → min-max inputs
    SELECT = "select"  # boolean / term → option list
    TEXT = "text"  # text / url → contains


_FACET_KINDS: dict[FieldType, FacetKind] = {
    FieldType.NUMBER: FacetKind.RANGE,
    FieldType.DATE: FacetKind.RANGE,
    FieldType.BOOLEAN: FacetKind.SELECT,
    FieldType.TERM: FacetKind.SELECT,
    FieldType.TEXT: FacetKind.TEXT,
    FieldType.URL: FacetKind.TEXT,
}

_IMPLICIT_FEATURE_COLUMNS = frozenset(spec.name for spec in IMPLICIT_FEATURE_COLUMN_SPECS)


class Facet(BaseModel):
    """One derivable filter control, addressed by its FilterExpr dotted path."""

    field: str  # dotted FilterExpr ref, e.g. "metadata.mass"
    label: str  # bare column/field name for display
    kind: FacetKind
    type: FieldType
    description: str | None = None
    unit: str | None = None


class FilterPanelData(BaseModel):
    """Facet controls for one table, derived purely from the schema manifest.

    Facet ``field`` values are the dotted paths the ``FilterExpr`` grammar
    accepts, so a consumer emits valid filters without further mapping:

    - records table → one facet per schema metadata field (implicit columns
      like ``created_at`` are not FilterExpr-addressable, so they never facet);
    - feature table → one facet per hook-declared column (implicit ``id`` /
      ``record_srn`` / ``created_at`` are skipped for the same reason).
    """

    schema: str
    table: str
    facets: list[Facet]

    @classmethod
    def from_manifest(cls, manifest: SchemaManifest, table: str) -> FilterPanelData:
        """Derive the facet controls for one table of *manifest*.

        Raises :class:`NotFoundError` when *table* is not a resource of the
        schema.
        """
        if table == RECORDS_TABLE:
            facets = [
                Facet(
                    field=f"metadata.{field.name}",
                    label=field.name,
                    kind=_FACET_KINDS[field.type],
                    type=field.type,
                    description=field.description,
                    unit=field.unit,
                )
                for field in manifest.fields
            ]
            return cls(schema=manifest.id, table=table, facets=facets)

        resource = next(
            (
                r
                for r in manifest.table_resources
                if r.name == table and r.kind == TableKind.FEATURE
            ),
            None,
        )
        if resource is None:
            raise NotFoundError(f"Schema {manifest.id!r} has no table {table!r}")

        facets = [
            Facet(
                field=f"features.{table}.{col.name}",
                label=col.name,
                kind=_FACET_KINDS[col.type],
                type=col.type,
                description=col.description,
                unit=col.unit,
            )
            for col in resource.columns
            if col.name not in _IMPLICIT_FEATURE_COLUMNS
        ]
        return cls(schema=manifest.id, table=table, facets=facets)
