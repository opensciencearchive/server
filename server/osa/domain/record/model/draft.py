"""RecordDraft — value object for publishing a record from any source."""

from typing import Any

from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSlug
from osa.domain.shared.model.value import ValueObject
from osa.domain.shared.model.hook import FeatureName


class RecordDraft(ValueObject):
    """Input to RecordService.publish_record().

    Carries everything needed to create a Record from any source type.
    ``expected_features`` lists feature table names (not full HookIdentitys)
    so compute runtime details don't leak past the validation boundary.
    """

    source: RecordSource
    metadata: dict[str, Any]
    convention_id: ConventionSlug
    expected_features: list[FeatureName] = []
