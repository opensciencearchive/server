"""Keyset pagination helpers with correct NULL semantics.

Derives both ORDER BY and WHERE predicate from a single sort specification
so that NULL handling is consistent between the two.

Key insight for NULLS LAST ordering:
- Non-null cursor value: "strictly after" must include ``OR expr IS NULL``
  because NULLs sort after all non-null values.
- Null cursor value: only the tiebreaker applies
  (``sort IS NULL AND id > cursor_id``), since nothing comes after the
  NULL region except more NULLs distinguished by the tiebreaker.

NULLS FIRST is the mirror image.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from sqlalchemy import ColumnElement, UnaryExpression, and_, false, or_


@dataclass(frozen=True)
class SortKey:
    """One column in a multi-column keyset sort."""

    expression: ColumnElement[Any]
    descending: bool = False
    nulls_last: bool = True

    def order_clause(self) -> UnaryExpression[Any]:
        clause = self.expression.desc() if self.descending else self.expression.asc()
        return clause.nullslast() if self.nulls_last else clause.nullsfirst()


class KeysetPage:
    """Build ORDER BY + WHERE predicate for keyset pagination.

    Usage::

        page = KeysetPage([
            SortKey(sort_expr, descending=is_desc, nulls_last=True),
            SortKey(t.c.id, descending=is_desc),
        ])
        stmt = stmt.order_by(*page.order_by())
        if cursor:
            stmt = stmt.where(page.after(cursor_values))
    """

    def __init__(self, keys: Sequence[SortKey]) -> None:
        self._keys = list(keys)

    def order_by(self) -> list[UnaryExpression[Any]]:
        return [k.order_clause() for k in self._keys]

    def after(self, cursor_values: tuple[Any, ...]) -> ColumnElement[Any]:
        """Build the WHERE predicate for "rows strictly after this cursor"."""
        if len(cursor_values) != len(self._keys):
            raise ValueError(
                f"Cursor length {len(cursor_values)} does not match key length {len(self._keys)}"
            )

        # Build from right to left:  for keys (k0, k1), the predicate is
        #   strictly_after(k0, v0) OR (eq(k0, v0) AND strictly_after(k1, v1))
        result: ColumnElement[Any] = false()
        for i in range(len(self._keys) - 1, -1, -1):
            after_i = _strictly_after(self._keys[i], cursor_values[i])
            if after_i is None:
                # No rows can follow on this key alone (null + nulls_last)
                # but tiebreaker may still apply via the eq branch below
                eq_part = _null_eq(self._keys[i].expression, cursor_values[i])
                result = and_(eq_part, result)
            elif i == len(self._keys) - 1:
                result = after_i
            else:
                eq_part = _null_eq(self._keys[i].expression, cursor_values[i])
                result = or_(after_i, and_(eq_part, result))

        return result


def _null_eq(expr: ColumnElement[Any], value: Any) -> ColumnElement[Any]:
    """``IS NULL`` when value is None, else ``= value``."""
    if value is None:
        return expr.is_(None)
    return expr == value


def _strictly_after(key: SortKey, value: Any) -> ColumnElement[Any] | None:
    """Rows that come strictly after *value* according to this key's ordering.

    Returns ``None`` when no rows can follow (null cursor + nulls_last).
    """
    expr = key.expression

    if value is None:
        # Cursor is at the NULL region
        if key.nulls_last:
            # NULLs are last → nothing comes after
            return None
        else:
            # NULLs are first → everything non-null comes after
            return expr.is_not(None)

    # Cursor is at a non-null value
    gt = expr < value if key.descending else expr > value

    if key.nulls_last:
        # NULLs come after all non-nulls → include them
        return or_(gt, expr.is_(None))
    else:
        # NULLs came before all non-nulls → they're already passed
        return gt
