"""ConventionRegistered event - emitted when a new convention is created."""

from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaId


class ConventionRegistered(Event):
    """Emitted when a convention is created via deploy.

    Carries hook definitions so ``CreateFeatureTables`` can create feature
    tables without querying the convention repository.

    Carries ``schema_id`` and ``schema_fields`` so ``EnsureMetadataTable`` can
    create and evolve typed metadata tables without traversing the semantics
    repository.
    """

    id: EventId
    convention_srn: ConventionSRN
    schema_id: SchemaId
    schema_fields: list[FieldDefinition] = []
    hooks: list[HookDefinition] = []
