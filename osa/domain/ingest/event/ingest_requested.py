"""IngestRequested event - triggers ingestion from an upstream source."""

from datetime import datetime

from osa.domain.shared.event import Event, EventId


class IngestRequested(Event):
    """Emitted when ingestion should start for a source.

    The `since` field is always set by the emitter (scheduler or initial run listener)
    based on the last IngestionRunCompleted event, or None for first run.
    """

    id: EventId
    ingestor_name: str  # Key into config.ingestors dict (e.g., "geo")
    since: datetime | None  # Fetch records updated after this time (None = all time)
    limit: int | None = None  # Optional limit on records to fetch
