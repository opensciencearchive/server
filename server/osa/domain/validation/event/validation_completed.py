from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, ValidationRunSRN
from osa.domain.validation.model import RunStatus


class ValidationCompleted(Event):
    """Emitted when validation finishes for a deposition."""

    id: EventId
    validation_run_srn: ValidationRunSRN
    deposition_srn: DepositionSRN
    convention_srn: ConventionSRN
    status: RunStatus
    hook_results: list[dict[str, Any]]
    metadata: dict[str, Any]
