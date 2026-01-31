"""Pydantic models for the CLI.

These are CLI-specific models, separate from the server domain models.
Keeping them here allows the CLI to be extracted to its own package.
"""

from pydantic import BaseModel


class RecordMetadata(BaseModel):
    """Metadata for a record."""

    title: str = "Untitled"
    summary: str | None = None
    organism: str | None = None
    sample_count: str | None = None
    pub_date: str | None = None
    platform: str | None = None
    gds_type: str | None = None
    entry_type: str | None = None

    model_config = {"extra": "allow"}  # Allow additional fields


class SearchHit(BaseModel):
    """A single search result."""

    srn: str
    short_id: str
    score: float = 0.0
    metadata: RecordMetadata


class SearchCache(BaseModel):
    """Cached search results for numbered lookup."""

    index: str
    query: str
    searched_at: str
    results: list[SearchHit]
