from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN


class FileDeletedEvent(Event):
    """Emitted when a file is deleted from a deposition."""

    id: EventId
    deposition_id: DepositionSRN
    filename: str
