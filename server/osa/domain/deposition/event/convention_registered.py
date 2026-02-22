"""ConventionRegistered event - emitted when a new convention is created."""

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN


class ConventionRegistered(Event):
    """Emitted when a convention is created via deploy.

    Downstream handlers (e.g. TriggerSourceOnDeploy) react to this
    to kick off initial source runs without waiting for a server restart.
    """

    id: EventId
    convention_srn: ConventionSRN
