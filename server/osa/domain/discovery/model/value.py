"""Discovery domain value objects — filters, cursors, result types."""

from __future__ import annotations

import base64
import json
from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel

from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.model.srn import RecordSRN


class FilterOperator(StrEnum):
    EQ = "eq"
    CONTAINS = "contains"
    GTE = "gte"
    LTE = "lte"


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


class Filter(BaseModel):
    field: str
    operator: FilterOperator
    value: str | float | bool


VALID_OPERATORS: dict[FieldType, set[FilterOperator]] = {
    FieldType.TEXT: {FilterOperator.EQ, FilterOperator.CONTAINS},
    FieldType.URL: {FilterOperator.EQ, FilterOperator.CONTAINS},
    FieldType.NUMBER: {FilterOperator.EQ, FilterOperator.GTE, FilterOperator.LTE},
    FieldType.DATE: {FilterOperator.EQ, FilterOperator.GTE, FilterOperator.LTE},
    FieldType.BOOLEAN: {FilterOperator.EQ},
    FieldType.TERM: {FilterOperator.EQ},
}


def encode_cursor(sort_value: Any, id_value: Any) -> str:
    """Encode a cursor as base64 JSON."""
    payload = {"s": sort_value, "id": id_value}
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_cursor(cursor: str) -> dict[str, Any]:
    """Decode a base64 JSON cursor. Raises ValueError on malformed input."""
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        data = json.loads(raw)
    except Exception as exc:
        raise ValueError(f"Malformed cursor: {exc}") from exc
    if not isinstance(data, dict) or "s" not in data or "id" not in data:
        raise ValueError("Cursor must contain 's' and 'id' keys")
    return data


class RecordSummary(BaseModel):
    srn: RecordSRN
    published_at: datetime
    metadata: dict[str, Any]


class RecordSearchResult(BaseModel):
    results: list[RecordSummary]
    cursor: str | None
    has_more: bool


class ColumnInfo(BaseModel):
    name: str
    type: str
    required: bool


class FeatureCatalogEntry(BaseModel):
    hook_name: str
    columns: list[ColumnInfo]
    record_count: int


class FeatureCatalog(BaseModel):
    tables: list[FeatureCatalogEntry]


class FeatureRow(BaseModel):
    row_id: int
    record_srn: RecordSRN
    data: dict[str, Any]


class FeatureSearchResult(BaseModel):
    rows: list[FeatureRow]
    cursor: str | None
    has_more: bool
