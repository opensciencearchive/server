"""IngestionRunCompleted event - emitted after an ingestion run finishes."""

from datetime import datetime

from osa.domain.shared.event import Event, EventId


class IngestionRunCompleted(Event):
    """Emitted after an ingestion run completes successfully."""

    id: EventId
    ingestor_name: str
    source_type: str  # "geo", "ena", etc.
    started_at: datetime
    completed_at: datetime
    record_count: int
    since: datetime | None  # What 'since' value was used (None = all time)
    limit: int | None
