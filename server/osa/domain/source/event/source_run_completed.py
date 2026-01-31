"""SourceRunCompleted event - emitted after a source run finishes."""

from datetime import datetime

from osa.domain.shared.event import Event, EventId


class SourceRunCompleted(Event):
    """Emitted after a source run completes successfully.

    For chunked processing:
    - `offset`: The starting position of this chunk
    - `chunk_size`: Chunk size used for processing
    - `is_final_chunk`: Whether this was the last chunk
    """

    id: EventId
    source_name: str
    source_type: str  # "geo-entrez", "ena", etc.
    started_at: datetime
    completed_at: datetime
    record_count: int
    since: datetime | None  # What 'since' value was used (None = all time)
    limit: int | None
    offset: int = 0  # Starting position of this chunk
    chunk_size: int = 1000  # Chunk size used
    is_final_chunk: bool = True  # Whether this was the last chunk
