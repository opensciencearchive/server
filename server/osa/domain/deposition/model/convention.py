from datetime import datetime

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN


class Convention(Aggregate):
    """An immutable, user-facing submission template."""

    srn: ConventionSRN
    title: str
    description: str | None = None
    schema_srn: SchemaSRN
    file_requirements: FileRequirements
    hooks: list[HookDefinition] = []
    source: SourceDefinition | None = None
    created_at: datetime
