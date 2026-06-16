from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSlug, DepositionSRN, ValidationRunSRN
from osa.domain.validation.model import RunStatus
from osa.domain.shared.model.hook import FeatureName


class ValidationCompleted(Event):
    """Emitted when validation finishes for a deposition."""

    id: EventId
    validation_run_srn: ValidationRunSRN
    deposition_srn: DepositionSRN
    convention_id: ConventionSlug
    status: RunStatus
    hook_results: list[dict[str, Any]]
    metadata: dict[str, Any]
    expected_features: list[FeatureName] = []
