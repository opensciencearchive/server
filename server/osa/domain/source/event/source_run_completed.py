"""SourceRunCompleted event - emitted after a source run finishes."""

from datetime import datetime

from osa.domain.shared.event import Event, EventId


class SourceRunCompleted(Event):
    """Emitted after a source run completes successfully."""

    id: EventId
    source_name: str
    source_type: str  # "geo-entrez", "ena", etc.
    started_at: datetime
    completed_at: datetime
    record_count: int
    since: datetime | None  # What 'since' value was used (None = all time)
    limit: int | None
