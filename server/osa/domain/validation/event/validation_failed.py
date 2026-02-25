from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.validation.model import RunStatus


class ValidationFailed(Event):
    """Emitted when validation fails for a deposition."""

    id: EventId
    deposition_srn: DepositionSRN
    convention_srn: ConventionSRN
    status: RunStatus
    reasons: list[str]
