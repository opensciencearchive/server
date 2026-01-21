"""SourceRequested event - triggers pulling from a data source."""

from datetime import datetime
from typing import Any

from osa.domain.shared.event import Event, EventId


class SourceRequested(Event):
    """Emitted when pulling should start for a source.

    The `since` field is always set by the emitter (scheduler or initial run listener)
    based on the last SourceRunCompleted event, or None for first run.

    For chunked processing:
    - `offset`: Starting position for this chunk (0 for first chunk)
    - `chunk_size`: Number of records to process per chunk
    - `session`: Opaque pagination state (e.g., NCBI WebEnv) for efficient continuation
    """

    id: EventId
    source_name: str  # Key into config.sources list (e.g., "geo-entrez")
    since: datetime | None  # Fetch records updated after this time (None = all time)
    limit: int | None = None  # Optional limit on records to fetch
    offset: int = 0  # Starting position for this chunk
    chunk_size: int = 1000  # Number of records per chunk
    session: dict[str, Any] | None = None  # Opaque pagination state (e.g., NCBI WebEnv)
