from datetime import datetime

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.domain.shared.model.validator import ValidatorRef


class Convention(Aggregate):
    """An immutable, user-facing submission template."""

    srn: ConventionSRN
    title: str
    description: str | None = None
    schema_srn: SchemaSRN
    file_requirements: FileRequirements
    validator_refs: list[ValidatorRef] = []
    created_at: datetime
