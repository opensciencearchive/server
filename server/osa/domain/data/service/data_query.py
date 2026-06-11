"""DataQueryService — streaming read business logic for records and features.

Validates the filter tree's bounds (depth, predicate count, distinct feature
joins) against config, then delegates row streaming to the
:class:`DataReadStore` port. The returned async iterators are wrapped by the
route layer in a serializer + ``StreamingResponse``. Validation runs at the top
of the generator body, so it surfaces on the route's pre-flight ``__anext__``
pull — before any response bytes (research §4).

Field/operator-compatibility validation against the resolved table's columns is
performed by the read-store adapter during SQL compilation (where the column
types are known); that path raises ``ValidationError`` before the first row.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterator, Mapping
from datetime import timedelta
from typing import Any

from osa.config import Config
from osa.domain.data.model.filter import And, FeatureFieldRef, FilterExpr, Not, Or, Predicate
from osa.domain.data.model.query_plan import QueryPlan, TableKind
from osa.domain.data.port.data_read_store import DataReadStore
from osa.domain.shared.error import ValidationError
from osa.domain.shared.service import Service


class DataQueryService(Service):
    read_store: DataReadStore
    config: Config

    async def stream_records(
        self, plan: QueryPlan, timeout: timedelta | None = None
    ) -> AsyncIterator[Mapping[str, Any]]:
        if plan.table_kind != TableKind.RECORDS:
            raise ValidationError("stream_records requires a RECORDS plan", field="table_kind")
        self._validate_filter_bounds(plan.filter)
        async for row in self.read_store.stream_rows(plan, timeout):
            yield row

    async def stream_features(
        self, plan: QueryPlan, timeout: timedelta | None = None
    ) -> AsyncIterator[Mapping[str, Any]]:
        if plan.table_kind != TableKind.FEATURE:
            raise ValidationError("stream_features requires a FEATURE plan", field="table_kind")
        self._validate_filter_bounds(plan.filter)
        async for row in self.read_store.stream_rows(plan, timeout):
            yield row

    # ------------------------------------------------------------------ #
    # Filter-tree bounds (ported from DiscoveryService, config-driven)
    # ------------------------------------------------------------------ #

    def _validate_filter_bounds(self, expr: FilterExpr | None) -> None:
        if expr is None:
            return
        depth = _tree_depth(expr)
        if depth > self.config.data.max_filter_depth:
            raise ValidationError(
                f"Filter tree depth {depth} exceeds maximum {self.config.data.max_filter_depth}.",
                field="filter",
                code="filter_depth_exceeded",
            )
        predicates = list(_iter_predicates(expr))
        if len(predicates) > self.config.data.max_predicates:
            raise ValidationError(
                f"Filter tree has {len(predicates)} predicates, exceeds maximum "
                f"{self.config.data.max_predicates}.",
                field="filter",
                code="filter_predicates_exceeded",
            )
        distinct_hooks = {p.field.hook for p in predicates if isinstance(p.field, FeatureFieldRef)}
        if len(distinct_hooks) > self.config.data.max_feature_joins:
            raise ValidationError(
                f"Filter joins {len(distinct_hooks)} feature hooks, exceeds maximum "
                f"{self.config.data.max_feature_joins}.",
                field="filter",
                code="filter_joins_exceeded",
            )


def _tree_depth(expr: FilterExpr) -> int:
    if isinstance(expr, Predicate):
        return 1
    if isinstance(expr, Not):
        return 1 + _tree_depth(expr.operand)
    if isinstance(expr, (And, Or)):
        return 1 + max(_tree_depth(op) for op in expr.operands)
    return 1


def _iter_predicates(expr: FilterExpr) -> Iterator[Predicate]:
    if isinstance(expr, Predicate):
        yield expr
    elif isinstance(expr, Not):
        yield from _iter_predicates(expr.operand)
    elif isinstance(expr, (And, Or)):
        for op in expr.operands:
            yield from _iter_predicates(op)
