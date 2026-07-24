"""Shared request parsing for table routes — sort spec + filter body."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict

from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.query_plan import SortDirection, SortSpec
from osa.domain.shared.error import ValidationError


class FilterRequestBody(BaseModel):
    """POST body shared by every table format (records + feature).

    ``extra="forbid"``: a mis-shaped body (e.g. an agent copying a wrong filter
    DSL) must 422 at the edge rather than be silently dropped and the read run
    unfiltered — a wrong-but-plausible 200 is the worst outcome for an agent.
    """

    model_config = ConfigDict(extra="forbid")

    filter: FilterExpr | None = None
    cursor: str | None = None
    # Unbounded at the edge: the table-read handler clamps to
    # [1, DataConfig.max_page_limit] — over-large requests get the max page, not a 422.
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
