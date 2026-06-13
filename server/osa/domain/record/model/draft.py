"""RecordDraft — value object for publishing a record from any source."""

from typing import Any

from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionId
from osa.domain.shared.model.value import ValueObject


class RecordDraft(ValueObject):
    """Input to RecordService.publish_record().

    Carries everything needed to create a Record from any source type.
    ``expected_features`` lists feature table names (not full HookIdentitys)
    so compute runtime details don't leak past the validation boundary.
    """

    source: RecordSource
    metadata: dict[str, Any]
    convention_id: ConventionId
    expected_features: list[str] = []
    hook_run_ids: dict[str, str] = {}  # hook name → hook_runs.id (provenance, #145)
