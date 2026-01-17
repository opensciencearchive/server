"""SourceRequested event - triggers pulling from a data source."""

from datetime import datetime

from osa.domain.shared.event import Event, EventId


class SourceRequested(Event):
    """Emitted when pulling should start for a source.

    The `since` field is always set by the emitter (scheduler or initial run listener)
    based on the last SourceRunCompleted event, or None for first run.
    """

    id: EventId
    source_name: str  # Key into config.sources list (e.g., "geo-entrez")
    since: datetime | None  # Fetch records updated after this time (None = all time)
    limit: int | None = None  # Optional limit on records to fetch
