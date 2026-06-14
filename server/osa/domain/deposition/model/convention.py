from datetime import datetime

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSlug, SchemaId


class Convention(Aggregate):
    """An immutable, user-facing submission template.

    Feature #145: identified by a caller-supplied ``ConventionSlug``
    (``"<slug>@<version>"``) rather than an opaque server-generated SRN, and
    references its hooks by **name** — the versioned release each name resolves
    to lives in the ``validation`` hook registry, not inline here.
    """

    id: ConventionSlug
    title: str
    description: str | None = None
    schema_id: SchemaId
    file_requirements: FileRequirements
    hooks: list[HookName] = []
    ingester: IngesterDefinition | None = None
    created_at: datetime
