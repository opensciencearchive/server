from osa.domain.auth.model.value import UserId
from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionId, DepositionSRN


class DepositionCreatedEvent(Event):
    """Emitted when a new deposition is created."""

    id: EventId
    deposition_id: DepositionSRN
    convention_id: ConventionId
    owner_id: UserId
