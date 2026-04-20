"""ConventionRegistered event - emitted when a new convention is created."""

from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN


class ConventionRegistered(Event):
    """Emitted when a convention is created via deploy.

    Carries hook definitions so downstream handlers (e.g. CreateFeatureTables)
    can create feature tables without querying the convention repository.

    Carries ``schema_srn`` and ``schema_fields`` so downstream handlers (e.g.
    EnsureMetadataTable) can create and evolve typed metadata tables without
    traversing the semantics repository.
    """

    id: EventId
    convention_srn: ConventionSRN
    schema_srn: SchemaSRN
    schema_fields: list[FieldDefinition] = []
    hooks: list[HookDefinition] = []
