"""Tests for keyset pagination helpers — NULL × direction matrix."""

from __future__ import annotations

import pytest
from typing import Any

from sqlalchemy import column

from osa.infrastructure.persistence.keyset import KeysetPage, SortKey


def _compile(clause: Any) -> str:
    """Compile a SQLAlchemy clause element to a raw SQL string for assertions."""
    return str(clause.compile(compile_kwargs={"literal_binds": True}))


# ---------------------------------------------------------------------------
# SortKey.order_clause
# ---------------------------------------------------------------------------


class TestSortKeyOrderClause:
    def test_asc_nulls_last(self) -> None:
        key = SortKey(expression=column("score"), descending=False, nulls_last=True)
        sql = _compile(key.order_clause())
        assert "ASC" in sql
        assert "NULLS LAST" in sql

    def test_desc_nulls_last(self) -> None:
        key = SortKey(expression=column("score"), descending=True, nulls_last=True)
        sql = _compile(key.order_clause())
        assert "DESC" in sql
        assert "NULLS LAST" in sql

    def test_asc_nulls_first(self) -> None:
        key = SortKey(expression=column("score"), descending=False, nulls_last=False)
        sql = _compile(key.order_clause())
        assert "ASC" in sql
        assert "NULLS FIRST" in sql

    def test_desc_nulls_first(self) -> None:
        key = SortKey(expression=column("score"), descending=True, nulls_last=False)
        sql = _compile(key.order_clause())
        assert "DESC" in sql
        assert "NULLS FIRST" in sql


# ---------------------------------------------------------------------------
# KeysetPage.after — non-null cursor sort value
# ---------------------------------------------------------------------------


class TestKeysetAfterNonNull:
    """Cursor sort value is NOT None — standard keyset with NULL awareness."""

    def test_asc_nulls_last(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=False, nulls_last=True),
                SortKey(column("id"), descending=False),
            ]
        )
        sql = _compile(page.after((5, "abc")))
        # Must include OR score IS NULL (NULLs come after non-nulls)
        assert "score > 5" in sql
        assert "score IS NULL" in sql
        assert "id > 'abc'" in sql

    def test_desc_nulls_last(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=True, nulls_last=True),
                SortKey(column("id"), descending=True),
            ]
        )
        sql = _compile(page.after((5, "abc")))
        assert "score < 5" in sql
        assert "score IS NULL" in sql
        assert "id < 'abc'" in sql

    def test_asc_nulls_first(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=False, nulls_last=False),
                SortKey(column("id"), descending=False, nulls_last=False),
            ]
        )
        sql = _compile(page.after((5, "abc")))
        assert "score > 5" in sql
        # No score IS NULL — nulls already came before
        assert "score IS NULL" not in sql
        assert "id > 'abc'" in sql

    def test_desc_nulls_first(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=True, nulls_last=False),
                SortKey(column("id"), descending=True, nulls_last=False),
            ]
        )
        sql = _compile(page.after((5, "abc")))
        assert "score < 5" in sql
        assert "score IS NULL" not in sql
        assert "id < 'abc'" in sql


# ---------------------------------------------------------------------------
# KeysetPage.after — null cursor sort value (the core bug)
# ---------------------------------------------------------------------------


class TestKeysetAfterNull:
    """Cursor sort IS None — must avoid `sort > NULL` which is always false in SQL."""

    def test_asc_nulls_last_null_cursor(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=False, nulls_last=True),
                SortKey(column("id"), descending=False),
            ]
        )
        sql = _compile(page.after((None, "abc")))
        # Nothing after NULLs in NULLS LAST except by tiebreaker
        assert "score IS NULL" in sql
        assert "id > 'abc'" in sql
        # Must NOT contain score > or score <
        assert "score >" not in sql
        assert "score <" not in sql

    def test_desc_nulls_last_null_cursor(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=True, nulls_last=True),
                SortKey(column("id"), descending=True),
            ]
        )
        sql = _compile(page.after((None, "abc")))
        assert "score IS NULL" in sql
        assert "id < 'abc'" in sql
        assert "score >" not in sql
        assert "score <" not in sql

    def test_asc_nulls_first_null_cursor(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=False, nulls_last=False),
                SortKey(column("id"), descending=False),
            ]
        )
        sql = _compile(page.after((None, "abc")))
        # NULLs are first, so everything non-null comes after, plus tiebreaker among NULLs
        assert "IS NOT NULL" in sql
        assert "id > 'abc'" in sql

    def test_desc_nulls_first_null_cursor(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=True, nulls_last=False),
                SortKey(column("id"), descending=True),
            ]
        )
        sql = _compile(page.after((None, "abc")))
        assert "IS NOT NULL" in sql
        assert "id < 'abc'" in sql


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestKeysetEdgeCases:
    def test_mismatched_cursor_length_raises(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=False, nulls_last=True),
                SortKey(column("id"), descending=False),
            ]
        )
        with pytest.raises(ValueError, match="length"):
            page.after((1, 2, 3))

    def test_order_by_returns_all_keys(self) -> None:
        page = KeysetPage(
            [
                SortKey(column("score"), descending=False, nulls_last=True),
                SortKey(column("id"), descending=False),
            ]
        )
        clauses = page.order_by()
        assert len(clauses) == 2
