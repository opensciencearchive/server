from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN


class MetadataUpdatedEvent(Event):
    """Emitted when deposition metadata is updated."""

    id: EventId
    deposition_id: DepositionSRN
    metadata: dict[str, Any]
