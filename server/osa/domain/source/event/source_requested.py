"""SourceRequested event - triggers pulling from a data source."""

from datetime import datetime
from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN


class SourceRequested(Event):
    """Emitted when pulling should start for a source.

    The convention SRN identifies which convention (and its SourceDefinition)
    to run. The server loads the convention to get the source image/config.

    For chunked processing:
    - `offset`: Starting position for this chunk (0 for first chunk)
    - `chunk_size`: Number of records to process per chunk
    - `session`: Opaque pagination state for efficient continuation
    """

    id: EventId
    convention_srn: ConventionSRN
    since: datetime | None = None
    limit: int | None = None
    offset: int = 0
    chunk_size: int = 1000
    session: dict[str, Any] | None = None
