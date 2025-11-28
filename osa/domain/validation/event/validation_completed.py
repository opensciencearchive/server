from typing import Any
from osa.domain.shared.event import Event
from osa.domain.shared.model.srn import DepositionSRN


class ValidationCompleted(Event):
    deposition_id: DepositionSRN
    summary: dict[str, Any]  # Placeholder for actual summary object
