"""Tool argument models — the MCP wire schemas the host shows the model (#162).

These are edge DTOs only: each maps 1:1 onto a domain view query
(``domain/data/query/view.py``). They reuse the domain's ``FilterExpr`` /
``SortSpec`` types wholesale so the tool boundary enforces the exact same
filter grammar and operator rules as the REST ``/data/`` surface, and so the
JSON Schema handed to the model embeds the real filter union.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.query_plan import SortSpec
from osa.domain.data.model.view import RECORDS_TABLE, TablePage
from osa.domain.data.query.view import TABLE_PATTERN
from osa.domain.shared.model.ids import RecordRef

ChartKind = Literal["line", "scatter", "bar"]


class ListDatasetsArgs(BaseModel):
    """`list_datasets` takes no arguments."""


class DescribeDatasetArgs(BaseModel):
    schema: str = Field(description="Schema id, bare (`alloy-sample`) or versioned (`@1.0.0`).")


class ShowTableArgs(BaseModel):
    schema: str = Field(description="Schema id, bare or versioned.")
    table: str = Field(
        default=RECORDS_TABLE,
        pattern=TABLE_PATTERN,
        description='"records" or a feature-table name from the schema manifest.',
    )
    filter: FilterExpr | None = Field(
        default=None,
        description=(
            "Optional filter tree. Field refs are dotted paths: "
            "`metadata.<field>` (records table) or `features.<table>.<column>` "
            "(that same feature table)."
        ),
    )
    sort: list[SortSpec] = Field(default_factory=list)
    limit: int = Field(default=50, ge=1, description="Page size (server-clamped).")


class ShowChartArgs(ShowTableArgs):
    x: str = Field(description="Column name for the x axis.")
    y: str = Field(description="Column name for the y axis.")
    kind: ChartKind
    series: str | None = Field(
        default=None, description="Optional categorical column to group series by."
    )
    # Charts want as much of the table as one page allows; the server clamps.
    limit: int = Field(default=1000, ge=1)


class ShowRecordArgs(BaseModel):
    id: str = Field(description="Record id, bare (`<id>`, latest) or versioned (`<id>@<n>`).")

    @field_validator("id")
    @classmethod
    def _parseable(cls, v: str) -> str:
        RecordRef.parse(v)  # raises on a malformed version suffix
        return v

    @property
    def record_ref(self) -> RecordRef:
        return RecordRef.parse(self.id)


class ShowFilterPanelArgs(BaseModel):
    schema: str
    table: str = Field(default=RECORDS_TABLE, pattern=TABLE_PATTERN)


class FetchPageArgs(ShowTableArgs):
    """App-only paging/re-sort round-trip — ShowTableArgs plus a cursor."""

    cursor: str | None = None


class SampleValuesArgs(BaseModel):
    """App-only bounded column sample for facet options (no DISTINCT endpoint)."""

    schema: str
    table: str = Field(default=RECORDS_TABLE, pattern=TABLE_PATTERN)
    column: str = Field(pattern=TABLE_PATTERN)
    limit: int = Field(default=100, ge=1)


class ChartData(BaseModel):
    """`show_chart` payload: the chart parameters echoed over one bounded page.

    ``page.truncated`` means the chart reflects only the fetched slice — the
    widget must say so rather than silently presenting a partial picture.
    """

    kind: ChartKind
    x: str
    y: str
    series: str | None = None
    page: TablePage
