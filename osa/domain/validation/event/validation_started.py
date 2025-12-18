from osa.domain.shared.event import Event
from osa.domain.shared.model.srn import DepositionSRN, TraitSRN, ValidationRunSRN


class ValidationStarted(Event):
    """Emitted when validation begins for a deposition."""

    validation_run_srn: ValidationRunSRN
    deposition_srn: DepositionSRN
    trait_srns: list[TraitSRN]
