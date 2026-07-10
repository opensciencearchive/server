"""ConventionRegistered event - emitted when a new convention is created."""

from osa.domain.semantics.model.value import FieldDefinition
from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook import HookIdentity
from osa.domain.shared.model.srn import ConventionSlug, SchemaId


class ConventionRegistered(Event):
    """Emitted when a convention is created via deploy.

    Audit-only (#160): the former ``CreateFeatureTables`` / ``EnsureMetadataTable``
    subscribers are gone — feature-table creation is inlined at the deploy command
    handler and metadata projection is a synchronous dual-write. The event lives
    on for the ``/events`` changefeed and keeps its hook + schema payload so that
    record remains self-describing to changefeed consumers.
    """

    id: EventId
    convention_id: ConventionSlug
    schema_id: SchemaId
    schema_fields: list[FieldDefinition] = []
    hooks: list[HookIdentity] = []
