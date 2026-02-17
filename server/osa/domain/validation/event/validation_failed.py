from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN


class ValidationFailed(Event):
    """Emitted when validation fails for a deposition."""

    id: EventId
    deposition_srn: DepositionSRN
    reasons: list[str]
