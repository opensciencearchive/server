"""Filter DSL for the ``/data/`` read surface.

Relocated from the ``discovery`` domain (research §3 / FR-041): the
``FilterExpr`` discriminated union, its typed field references, and the
operator-compatibility tables are reused wholesale with no behaviour change.
During the discovery→data coexistence window (research §10) the ``data``
domain keeps its own copy so it has no dependency on ``discovery`` once the
latter is deleted.

Two kinds of field references are supported:

- :class:`MetadataFieldRef` — resolves to a column in
  ``metadata.<schema_slug>_v<major>``.
- :class:`FeatureFieldRef` — resolves to a column in ``features.<hook>``.

Wire format for a reference is a dotted path (``metadata.<field>`` or
``features.<hook>.<column>``). :func:`parse_field_ref` parses the wire form
into a typed reference and validates identifier shape.
"""

from __future__ import annotations

import re
from enum import StrEnum
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, Field, model_validator

from osa.domain.semantics.model.value import FieldType

_IDENT = re.compile(r"^[a-z][a-z0-9_]*$")


# ── Field references ──


class MetadataFieldRef(BaseModel):
    path: Literal["metadata"] = "metadata"
    field: str

    def dotted(self) -> str:
        return f"metadata.{self.field}"


class FeatureFieldRef(BaseModel):
    path: Literal["features"] = "features"
    hook: str
    column: str

    def dotted(self) -> str:
        return f"features.{self.hook}.{self.column}"


def parse_field_ref(dotted: str) -> "FieldRef":
    """Parse a dotted-path field reference into its typed form.

    Raises :class:`ValueError` when the path shape or identifier doesn't match
    the documented grammar.
    """
    if not isinstance(dotted, str):
        raise ValueError(f"Expected dotted string, got {type(dotted).__name__}")

    parts = dotted.split(".")
    if not parts:
        raise ValueError(f"Empty field reference: {dotted!r}")

    head = parts[0]
    if head == "metadata":
        if len(parts) != 2:
            raise ValueError(f"metadata.* refs must be exactly two dotted parts, got {dotted!r}")
        field = parts[1]
        if not _IDENT.match(field):
            raise ValueError(f"Invalid metadata field identifier: {field!r}")
        return MetadataFieldRef(field=field)

    if head == "features":
        if len(parts) != 3:
            raise ValueError(f"features.* refs must be exactly three dotted parts, got {dotted!r}")
        hook, column = parts[1], parts[2]
        if not _IDENT.match(hook):
            raise ValueError(f"Invalid hook identifier: {hook!r}")
        if not _IDENT.match(column):
            raise ValueError(f"Invalid feature column identifier: {column!r}")
        return FeatureFieldRef(hook=hook, column=column)

    raise ValueError(
        f"Unknown field reference prefix {head!r} in {dotted!r}. "
        "Expected 'metadata.<field>' or 'features.<hook>.<column>'."
    )


# ── Operators ──


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


# ── Filter expression tree ──


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

And.model_rebuild()
Or.model_rebuild()
Not.model_rebuild()


# ── Operator compatibility tables ──

# Operators valid per semantic column type for metadata/feature validation.
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

# Operators valid against raw JSON-schema primitive types (feature columns
# whose Column.json_type is a JSON Schema primitive rather than a FieldType).
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
