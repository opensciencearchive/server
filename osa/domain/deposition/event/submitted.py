from osa.domain.shared.event import Event
from osa.domain.shared.model.srn import DepositionSRN


class DepositionSubmittedEvent(Event):
    deposition_id: DepositionSRN
