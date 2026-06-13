from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionId, DepositionSRN, ValidationRunSRN
from osa.domain.validation.model import RunStatus


class ValidationCompleted(Event):
    """Emitted when validation finishes for a deposition."""

    id: EventId
    validation_run_srn: ValidationRunSRN
    deposition_srn: DepositionSRN
    convention_id: ConventionId
    status: RunStatus
    hook_results: list[dict[str, Any]]
    metadata: dict[str, Any]
    expected_features: list[str] = []
    hook_run_ids: dict[str, str] = {}  # hook name → hook_runs.id (provenance, #145)
