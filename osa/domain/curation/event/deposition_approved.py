"""DepositionApproved event - emitted when a deposition passes curation."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN


class DepositionApproved(Event):
    """Emitted when a deposition is approved for publication."""

    id: EventId
    deposition_srn: DepositionSRN
    metadata: dict[str, Any]  # Descriptive info to publish
