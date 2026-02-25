"""SourceRunCompleted event - emitted after a source run finishes."""

from datetime import datetime

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN


class SourceRunCompleted(Event):
    """Emitted after a source run completes."""

    id: EventId
    convention_srn: ConventionSRN
    started_at: datetime
    completed_at: datetime
    record_count: int
    is_final_chunk: bool = True
