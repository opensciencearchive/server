"""IndexRecord event - per-backend indexing request for a single record."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import RecordSRN


class IndexRecord(Event):
    """Request to index a single record into a specific backend.

    This event is created by FanOutToIndexBackends when a RecordPublished
    event is received. Each backend gets its own IndexRecord event,
    enabling independent retry and failure isolation.

    Attributes:
        id: Unique event identifier (inherited from Event).
        backend_name: Target backend name (e.g., "vector", "keyword").
        record_srn: Structured Resource Name of the record.
        metadata: Record metadata to index.
        routing_key: Optional routing key for worker filtering (typically matches backend_name).
    """

    id: EventId
    backend_name: str
    record_srn: RecordSRN
    metadata: dict[str, Any]
    routing_key: str | None = None
