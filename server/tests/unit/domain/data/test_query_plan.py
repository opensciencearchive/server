"""T029 — QueryPlan validation rules and per-kind default sorts."""

import pytest
from pydantic import ValidationError as PydanticValidationError

from osa.domain.data.model.query_plan import (
    PaginationParams,
    QueryPlan,
    SortDirection,
    TableKind,
    decode_cursor,
    encode_cursor,
)
from osa.domain.shared.model.srn import SchemaId

SCHEMA = SchemaId.parse("compound@1.0.0")


def test_records_plan_defaults_to_created_at_id_desc() -> None:
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS)
    assert [(s.column, s.direction) for s in plan.sort] == [
        ("created_at", SortDirection.DESC),
        ("id", SortDirection.DESC),
    ]


def test_feature_plan_defaults_to_id_asc() -> None:
    plan = QueryPlan(
        schema_id=SCHEMA, table_kind=TableKind.FEATURE, feature_name="chemical_features"
    )
    assert [(s.column, s.direction) for s in plan.sort] == [("id", SortDirection.ASC)]


def test_feature_kind_requires_feature_name() -> None:
    with pytest.raises(PydanticValidationError):
        QueryPlan(schema_id=SCHEMA, table_kind=TableKind.FEATURE)


def test_records_kind_rejects_feature_name() -> None:
    with pytest.raises(PydanticValidationError):
        QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS, feature_name="x")


def test_limit_capped_at_1000() -> None:
    with pytest.raises(PydanticValidationError):
        PaginationParams(limit=5000)


def test_limit_default_is_50() -> None:
    assert PaginationParams().limit == 50


def test_cursor_roundtrip() -> None:
    enc = encode_cursor("2026-01-01T00:00:00", "abc")
    decoded = decode_cursor(enc)
    assert decoded == {"s": "2026-01-01T00:00:00", "id": "abc"}


def test_decode_malformed_cursor_raises() -> None:
    with pytest.raises(ValueError):
        decode_cursor("not-valid-base64-json!!!")
