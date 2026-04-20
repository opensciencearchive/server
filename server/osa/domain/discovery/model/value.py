"""Discovery domain value objects — filters, cursors, result types.

Feature 076 replaces the flat ``Filter`` list with a compound ``FilterExpr``
discriminated union (``And``/``Or``/``Not``/``Predicate``). Field references
inside predicates are typed (:class:`MetadataFieldRef` or
:class:`FeatureFieldRef`); the dotted wire form is parsed at the API boundary.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, model_validator

from osa.domain.discovery.model.refs import (
    FeatureFieldRef,
    MetadataFieldRef,
    parse_field_ref,
)
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.model.srn import RecordSRN


class FilterOperator(StrEnum):
    EQ = "eq"
    NEQ = "neq"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"
    IN = "in"
    CONTAINS = "contains"
    IS_NULL = "is_null"


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


FieldRef = Annotated[
    Union[MetadataFieldRef, FeatureFieldRef],
    Field(discriminator="path"),
]


PredicateValue = Union[str, int, float, bool, list[str], list[float], None]


class Predicate(BaseModel):
    kind: Literal["predicate"] = "predicate"
    field: FieldRef
    op: FilterOperator
    value: PredicateValue = None

    @model_validator(mode="before")
    @classmethod
    def _coerce_field(cls, data: Any) -> Any:
        """Accept dotted-path strings for ``field`` and parse them into the typed form."""
        if isinstance(data, dict):
            raw = data.get("field")
            if isinstance(raw, str):
                data = {**data, "field": parse_field_ref(raw)}
        return data


class And(BaseModel):
    kind: Literal["and"] = "and"
    operands: list["FilterExpr"] = Field(min_length=2)


class Or(BaseModel):
    kind: Literal["or"] = "or"
    operands: list["FilterExpr"] = Field(min_length=2)


class Not(BaseModel):
    kind: Literal["not"] = "not"
    operand: "FilterExpr"


FilterExpr = Annotated[
    Union[And, Or, Not, Predicate],
    Field(discriminator="kind"),
]

# Resolve forward references
And.model_rebuild()
Or.model_rebuild()
Not.model_rebuild()


# Operators valid per column type for metadata/feature column validation.
VALID_OPERATORS: dict[FieldType, set[FilterOperator]] = {
    FieldType.TEXT: {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.IN,
        FilterOperator.CONTAINS,
        FilterOperator.IS_NULL,
    },
    FieldType.URL: {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.IN,
        FilterOperator.CONTAINS,
        FilterOperator.IS_NULL,
    },
    FieldType.TERM: {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.IN,
        FilterOperator.IS_NULL,
    },
    FieldType.NUMBER: {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.GT,
        FilterOperator.GTE,
        FilterOperator.LT,
        FilterOperator.LTE,
        FilterOperator.IN,
        FilterOperator.IS_NULL,
    },
    FieldType.DATE: {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.GT,
        FilterOperator.GTE,
        FilterOperator.LT,
        FilterOperator.LTE,
        FilterOperator.IN,
        FilterOperator.IS_NULL,
    },
    FieldType.BOOLEAN: {FilterOperator.EQ, FilterOperator.IS_NULL},
}

# Operators valid against raw JSON-schema primitive types (used for feature columns
# whose Column.json_type is a JSON Schema primitive rather than a semantic FieldType).
JSON_TYPE_OPERATORS: dict[str, set[FilterOperator]] = {
    "string": {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.IN,
        FilterOperator.CONTAINS,
        FilterOperator.IS_NULL,
    },
    "number": {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.GT,
        FilterOperator.GTE,
        FilterOperator.LT,
        FilterOperator.LTE,
        FilterOperator.IN,
        FilterOperator.IS_NULL,
    },
    "integer": {
        FilterOperator.EQ,
        FilterOperator.NEQ,
        FilterOperator.GT,
        FilterOperator.GTE,
        FilterOperator.LT,
        FilterOperator.LTE,
        FilterOperator.IN,
        FilterOperator.IS_NULL,
    },
    "boolean": {FilterOperator.EQ, FilterOperator.IS_NULL},
    "array": {FilterOperator.EQ, FilterOperator.IS_NULL},
    "object": {FilterOperator.EQ, FilterOperator.IS_NULL},
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
