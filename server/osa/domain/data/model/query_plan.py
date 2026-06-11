"""Query IR for the ``/data/`` read surface.

``QueryPlan`` is the internal intermediate representation produced by both URL
parsing and POST-body parsing and consumed by the read engine. It is the pure
query description; the *output format* (which serializer, which statement
timeout) is intentionally NOT part of the plan — it is a route/serialization
concern carried alongside the plan, which also avoids a query_plan→format→
serializer→manifest→query_plan import cycle.

Also hosts the opaque cursor codec relocated from the discovery domain
(research §3 / FR-041) with no behaviour change.
"""

from __future__ import annotations

import base64
import json
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, model_validator

from osa.domain.data.model.filter import FilterExpr
from osa.domain.shared.model.ids import HookName
from osa.domain.shared.model.srn import SchemaId


class TableKind(StrEnum):
    RECORDS = "records"
    FEATURE = "feature"


class SortDirection(StrEnum):
    ASC = "asc"
    DESC = "desc"


class SortSpec(BaseModel):
    """A single sort key — column plus direction (no bare tuples at boundaries)."""

    column: str
    direction: SortDirection


class PaginationCursor(BaseModel):
    """Opaque base64 wrapper around the last row's ``(sort_value, id)`` pair."""

    value: str

    def __str__(self) -> str:
        return self.value


class PaginationParams(BaseModel):
    cursor: PaginationCursor | None = None
    limit: int = Field(default=50, ge=1, le=1000)


# Default sort keys per table kind (data-model.md §PaginationParams).
_DEFAULT_SORTS: dict[TableKind, list[SortSpec]] = {
    TableKind.RECORDS: [
        SortSpec(column="created_at", direction=SortDirection.DESC),
        SortSpec(column="id", direction=SortDirection.DESC),
    ],
    TableKind.FEATURE: [SortSpec(column="id", direction=SortDirection.ASC)],
}


class QueryPlan(BaseModel):
    schema_id: SchemaId
    table_kind: TableKind
    feature_name: HookName | None = None
    filter: FilterExpr | None = None
    pagination: PaginationParams = Field(default_factory=PaginationParams)
    sort: list[SortSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_and_default(self) -> "QueryPlan":
        # feature_name present iff FEATURE
        if self.table_kind == TableKind.FEATURE and self.feature_name is None:
            raise ValueError("feature_name is required when table_kind is FEATURE")
        if self.table_kind == TableKind.RECORDS and self.feature_name is not None:
            raise ValueError("feature_name must be None when table_kind is RECORDS")
        # Apply default sort per table kind when none was supplied.
        if not self.sort:
            self.sort = list(_DEFAULT_SORTS[self.table_kind])
        return self


def encode_cursor(sort_value: Any, id_value: Any) -> str:
    """Encode a cursor as urlsafe base64 of ``{"s": sort_value, "id": id_value}``.

    ``default=str`` because feature rows are raw DB mappings — a datetime sort
    value (``created_at``, or a date-time hook column) arrives unrendered. The
    sort decoders coerce the ISO string back to a datetime before binding.
    """
    payload = {"s": sort_value, "id": id_value}
    return base64.urlsafe_b64encode(json.dumps(payload, default=str).encode()).decode()


def decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode a base64 JSON cursor. Raises ``ValueError`` on malformed input."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
    except ValueError as exc:
        # binascii.Error and json.JSONDecodeError are both ValueError subclasses.
        raise ValueError(f"Malformed cursor: {exc}") from exc
    if not isinstance(data, dict) or "s" not in data or "id" not in data:
        raise ValueError("Cursor must contain 's' and 'id' keys")
    return data
