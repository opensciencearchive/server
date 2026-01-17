"""Type-safe result types for index queries."""

from typing import Any

from pydantic import BaseModel


class SearchHit(BaseModel, frozen=True):
    """A single search result from an index query."""

    srn: str
    score: float
    metadata: dict[str, Any]


class QueryResult(BaseModel, frozen=True):
    """Result of an index query."""

    hits: list[SearchHit]
    total: int
    query: str
