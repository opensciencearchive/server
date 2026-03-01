"""ConventionReady event — emitted after feature tables are created for a convention."""

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN


class ConventionReady(Event):
    """Emitted when feature tables have been created for a convention.

    Downstream handlers (e.g. TriggerInitialSourceRun) react to this
    to kick off initial source runs, knowing that feature tables are ready.
    """

    id: EventId
    convention_srn: ConventionSRN
