from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN, ValidationRunSRN


class ValidationStarted(Event):
    """Emitted when validation begins for a deposition."""

    id: EventId
    validation_run_srn: ValidationRunSRN
    deposition_srn: DepositionSRN
