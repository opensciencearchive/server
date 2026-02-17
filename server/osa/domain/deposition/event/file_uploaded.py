from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN


class FileUploadedEvent(Event):
    """Emitted when a file is uploaded to a deposition."""

    id: EventId
    deposition_id: DepositionSRN
    filename: str
    size: int
    checksum: str
