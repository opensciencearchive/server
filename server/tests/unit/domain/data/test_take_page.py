"""QueryPlan.take_page — the single owner of the limit+1/cursor mechanic (#162).

Shared by the REST paginated-JSON path and the view queries: consume up to
``limit + 1`` rows, flag truncation, and derive ``next_cursor`` from the last
returned row via the plan keyset.
"""

from collections.abc import AsyncIterator, Mapping
from typing import Any

from osa.domain.data.model.query_plan import (
    PaginationParams,
    QueryPlan,
    TableKind,
    decode_cursor,
)
from osa.domain.shared.model.srn import SchemaId


def _rows(items: list[dict[str, Any]]) -> AsyncIterator[Mapping[str, Any]]:
    async def gen() -> AsyncIterator[Mapping[str, Any]]:
        for item in items:
            yield item

    return gen()


def _plan(limit: int) -> QueryPlan:
    return QueryPlan(
        schema_id=SchemaId.parse("alloy-sample@1.0.0"),
        table_kind=TableKind.FEATURE,
        feature_name="tensile_test",
        pagination=PaginationParams(limit=limit),
    )


class TestTakePage:
    async def test_full_page_with_more_rows_sets_cursor_and_truncated(self):
        plan = _plan(limit=2)
        page = await plan.take_page(_rows([{"id": i} for i in range(3)]))
        assert len(page.rows) == 2
        assert page.truncated is True
        assert page.next_cursor is not None
        # Cursor derives from the LAST RETURNED row via the plan keyset
        # (feature default sort is id asc → sort value == id == tiebreak).
        assert decode_cursor(page.next_cursor)["id"] == 1

    async def test_last_page_has_no_cursor(self):
        page = await _plan(limit=2).take_page(_rows([{"id": 0}]))
        assert page.truncated is False
        assert page.next_cursor is None
        assert [r["id"] for r in page.rows] == [0]

    async def test_empty_page(self):
        page = await _plan(limit=2).take_page(_rows([]))
        assert page.rows == []
        assert page.next_cursor is None
        assert page.truncated is False

    async def test_exact_page_boundary_is_not_truncated(self):
        # Exactly `limit` rows available: the limit+1 probe finds nothing.
        page = await _plan(limit=2).take_page(_rows([{"id": 0}, {"id": 1}]))
        assert page.truncated is False
        assert page.next_cursor is None
