"""Unit tests for table-route request parsing (sort spec + plan build)."""

import pytest

from osa.application.api.v1.routes.data._params import build_plan, parse_sort
from osa.domain.data.model.query_plan import SortDirection, TableKind
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import SchemaId

SCHEMA = SchemaId.parse("compound@1.0.0")


def test_parse_sort_empty_is_empty_list() -> None:
    assert parse_sort(None) == []
    assert parse_sort("") == []


def test_parse_sort_multi_with_directions() -> None:
    specs = parse_sort("created_at:desc,id:desc")
    assert [(s.column, s.direction) for s in specs] == [
        ("created_at", SortDirection.DESC),
        ("id", SortDirection.DESC),
    ]


def test_parse_sort_bare_column_defaults_asc() -> None:
    specs = parse_sort("mw")
    assert specs[0].column == "mw"
    assert specs[0].direction == SortDirection.ASC


def test_parse_sort_invalid_direction_raises() -> None:
    with pytest.raises(ValidationError):
        parse_sort("mw:sideways")


def test_build_plan_wraps_cursor_and_limit() -> None:
    plan = build_plan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        feature_name=None,
        filter_expr=None,
        cursor="CUR",
        limit=10,
        sort="mw:asc",
    )
    assert plan.pagination.limit == 10
    assert str(plan.pagination.cursor) == "CUR"
    assert plan.sort[0].column == "mw"


def test_build_plan_no_cursor_is_none() -> None:
    plan = build_plan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        feature_name=None,
        filter_expr=None,
        cursor=None,
        limit=50,
        sort=None,
    )
    assert plan.pagination.cursor is None
    # default RECORDS sort applied
    assert plan.sort[0].column == "created_at"
