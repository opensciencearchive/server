"""Shared request parsing for table routes — sort spec, filter body, plan build."""

from __future__ import annotations

from pydantic import BaseModel

from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.query_plan import (
    PaginationCursor,
    PaginationParams,
    QueryPlan,
    SortDirection,
    SortSpec,
    TableKind,
)
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.ids import HookName
from osa.domain.shared.model.srn import SchemaId


class FilterRequestBody(BaseModel):
    """POST body shared by every table format (records + feature)."""

    filter: FilterExpr | None = None
    cursor: str | None = None
    # Unbounded at the edge: build_plan clamps to [1, DataConfig.max_page_limit]
    # via PaginationParams.clamped — over-large requests get the max page, not a 422.
    limit: int = 50
    sort: str | None = None


def parse_sort(raw: str | None) -> list[SortSpec]:
    """Parse ``col[:asc|:desc],col2[:asc|:desc]`` → SortSpec list (empty if None)."""
    if not raw:
        return []
    specs: list[SortSpec] = []
    for part in raw.split(","):
        token = part.strip()
        if not token:
            continue
        if ":" in token:
            column, direction = token.split(":", 1)
            try:
                dir_enum = SortDirection(direction.strip().lower())
            except ValueError as exc:
                raise ValidationError(
                    f"Invalid sort direction in {token!r}; expected 'asc' or 'desc'.",
                    field="sort",
                ) from exc
        else:
            column, dir_enum = token, SortDirection.ASC
        specs.append(SortSpec(column=column.strip(), direction=dir_enum))
    return specs


def build_plan(
    *,
    schema_id: SchemaId,
    table_kind: TableKind,
    feature_name: HookName | None,
    filter_expr: FilterExpr | None,
    cursor: str | None,
    limit: int,
    max_limit: int,
    sort: str | None,
) -> QueryPlan:
    pagination = PaginationParams.clamped(
        cursor=PaginationCursor(value=cursor) if cursor else None,
        limit=limit,
        max_limit=max_limit,
    )
    return QueryPlan(
        schema_id=schema_id,
        table_kind=table_kind,
        feature_name=feature_name,
        filter=filter_expr,
        pagination=pagination,
        sort=parse_sort(sort),
    )
