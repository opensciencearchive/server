"""``BoundedPage | FullStream`` — unbounded reads are opt-in by name (#219 phase 2).

The old ``PaginationParams`` carried a limit on every plan that the store
ignored and the CSV path disregarded — every reader had to know whether the
limit was real. The discriminated union encodes it structurally: interactive
paths can only construct ``BoundedPage`` (LIMIT pushed into SQL); ``FullStream``
exists solely for the dump serializers.
"""

import pytest

from osa.domain.data.model.query_plan import (
    BoundedPage,
    FullStream,
    PaginationCursor,
    QueryPlan,
    TableKind,
)
from osa.domain.shared.model.srn import SchemaId

SCHEMA = SchemaId.parse("compound@1.0.0")


def _plan(pagination) -> QueryPlan:
    return QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS, pagination=pagination)


class TestPaginationUnion:
    def test_default_pagination_is_a_bounded_page(self) -> None:
        plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS)
        assert isinstance(plan.pagination, BoundedPage)
        assert plan.pagination.limit == 50

    def test_full_stream_carries_no_limit_or_cursor(self) -> None:
        # The type has no such fields at all — "no limit" is unrepresentable
        # as a forgotten default.
        fields = set(FullStream.model_fields)
        assert "limit" not in fields and "cursor" not in fields

    def test_plan_accepts_full_stream(self) -> None:
        plan = _plan(FullStream())
        assert isinstance(plan.pagination, FullStream)

    def test_clamped_lives_on_bounded_page(self) -> None:
        page = BoundedPage.clamped(limit=5000, max_limit=1000)
        assert page.limit == 1000
        page = BoundedPage.clamped(limit=0, max_limit=1000)
        assert page.limit == 1

    def test_clamped_preserves_cursor(self) -> None:
        cur = PaginationCursor(value="abc")
        page = BoundedPage.clamped(cursor=cur, limit=10, max_limit=1000)
        assert page.cursor == cur


@pytest.mark.asyncio
class TestTakePageRequiresBoundedPage:
    async def test_take_page_on_full_stream_plan_is_a_programming_error(self) -> None:
        async def rows():
            yield {"srn": "urn:osa:localhost:rec:r1@1"}

        with pytest.raises(ValueError, match="BoundedPage"):
            await _plan(FullStream()).take_page(rows())
