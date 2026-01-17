"""Type-safe record type for ingested metadata."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class UpstreamRecord(BaseModel, frozen=True):
    """A record pulled from an upstream source."""

    source_id: str  # e.g., "GSE12345"
    source_type: str  # e.g., "geo"
    metadata: dict[str, Any]  # The actual metadata payload
    fetched_at: datetime
    source_url: str | None = None
