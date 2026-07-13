"""Compact log summaries for MCP tool calls (#162).

The dispatcher logs one line per tool call. These pure helpers turn the typed
arguments and result payloads into short, operator-readable strings — key
fields only, never a full filter tree or a row dump — so the log stays useful
without leaking bulk.
"""

from __future__ import annotations

from pydantic import BaseModel

from osa.application.api.mcp.models import ChartData
from osa.domain.data.model.manifest import SchemaManifest
from osa.domain.data.model.view import (
    ColumnSample,
    DatasetList,
    FilterPanelData,
    RecordDetailData,
    TablePage,
)

# Argument fields worth echoing, in display order. Bulk/opaque fields (filter,
# cursor) are flagged as present rather than dumped.
_ARG_FIELDS: tuple[str, ...] = (
    "schema",
    "table",
    "column",
    "id",
    "x",
    "y",
    "series",
    "kind",
    "limit",
)


def summarize_args(args: BaseModel) -> str:
    """A compact ``k=v`` view of a tool's arguments (no filter/cursor dumps)."""
    data = args.model_dump(exclude_none=True)
    parts = [f"{field}={data[field]}" for field in _ARG_FIELDS if field in data]
    if data.get("filter") is not None:
        parts.append("filter=yes")
    if data.get("cursor"):
        parts.append("cursor=yes")
    return " ".join(parts) if parts else "(no args)"


def summarize_result(payload: BaseModel) -> str:
    """A one-line outcome summary per payload type (counts, flags — no rows)."""
    if isinstance(payload, TablePage):
        return (
            f"{len(payload.rows)} rows, truncated={payload.truncated}, "
            f"next_page={payload.next_cursor is not None}"
        )
    if isinstance(payload, ChartData):
        return f"{payload.kind} chart, {len(payload.page.rows)} rows, truncated={payload.page.truncated}"
    if isinstance(payload, DatasetList):
        return f"{len(payload.datasets)} datasets"
    if isinstance(payload, RecordDetailData):
        return f"record {payload.record.id}, {len(payload.feature_tables)} feature tables"
    if isinstance(payload, FilterPanelData):
        return f"{len(payload.facets)} facets"
    if isinstance(payload, ColumnSample):
        return f"{len(payload.values)} values, truncated={payload.truncated}"
    if isinstance(payload, SchemaManifest):
        return (
            f"manifest {payload.id}@{payload.version}, {len(payload.fields)} fields, "
            f"{len(payload.table_resources)} tables"
        )
    return type(payload).__name__
