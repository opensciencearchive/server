"""RecordPublished event - emitted when a record is published and ready for indexing."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN


class RecordPublished(Event):
    """Emitted when a record is published and ready for indexing."""

    id: EventId
    record_srn: RecordSRN
    deposition_srn: DepositionSRN
    metadata: dict[str, Any]  # The record payload to index
