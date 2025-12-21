from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import DepositionSRN, ValidationRunSRN
from osa.domain.validation.model import CheckResult, RunStatus


class ValidationCompleted(Event):
    """Emitted when validation finishes for a deposition."""

    id: EventId
    validation_run_srn: ValidationRunSRN
    deposition_srn: DepositionSRN
    status: RunStatus
    results: list[CheckResult]
    metadata: dict[str, Any]  # Pass through the original metadata
