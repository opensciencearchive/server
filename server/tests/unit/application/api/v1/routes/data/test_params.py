"""Unit tests for table-route request parsing (sort spec). Plan construction
lives on the table-read handlers (tests/unit/domain/data/test_table_read_handlers.py)."""

import pytest

from osa.application.api.v1.routes.data._params import parse_sort
from osa.domain.data.model.query_plan import SortDirection
from osa.domain.shared.error import ValidationError


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
