from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN


class DepositionSubmittedEvent(Event):
    """Emitted when a deposition is submitted for validation."""

    id: EventId
    deposition_id: DepositionSRN
    metadata: dict[str, Any]  # Descriptive info to pass through pipeline
