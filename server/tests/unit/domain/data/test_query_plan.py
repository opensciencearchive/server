"""T029 — QueryPlan validation rules and per-kind default sorts."""

from datetime import UTC, datetime

import pytest
from pydantic import ValidationError as PydanticValidationError

from osa.domain.data.model.query_plan import (
    PaginationCursor,
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


def test_limit_above_max_clamps_to_max() -> None:
    # Clamp, don't reject: a consumer asking for "everything" with a big
    # number gets the max page, not a 422. The clamp *policy* lives HERE, on
    # the canonical model; the *bound* comes from config (DataConfig).
    assert PaginationParams.clamped(limit=5000, max_limit=1000).limit == 1000
    assert PaginationParams.clamped(limit=5000, max_limit=200).limit == 200


def test_limit_below_one_clamps_to_one() -> None:
    assert PaginationParams.clamped(limit=0, max_limit=1000).limit == 1
    assert PaginationParams.clamped(limit=-5, max_limit=1000).limit == 1


def test_clamped_preserves_cursor() -> None:
    p = PaginationParams.clamped(cursor=PaginationCursor(value="CUR"), limit=10, max_limit=1000)
    assert str(p.cursor) == "CUR"
    assert p.limit == 10


def test_limit_default_is_50() -> None:
    assert PaginationParams().limit == 50


def test_cursor_roundtrip() -> None:
    enc = encode_cursor("2026-01-01T00:00:00", "abc")
    decoded = decode_cursor(enc)
    assert decoded == {"s": "2026-01-01T00:00:00", "id": "abc"}


def test_decode_malformed_cursor_raises() -> None:
    with pytest.raises(ValueError):
        decode_cursor("not-valid-base64-json!!!")


def test_encode_cursor_accepts_datetime_sort_value() -> None:
    # Feature rows reach the cursor encoder as raw DB mappings, so a
    # created_at sort value is a datetime, not a pre-rendered string.
    ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    decoded = decode_cursor(encode_cursor(ts, 7))
    assert datetime.fromisoformat(decoded["s"]) == ts
    assert decoded["id"] == 7
