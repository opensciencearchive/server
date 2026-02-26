"""ConventionRegistered event - emitted when a new convention is created."""

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN


class ConventionRegistered(Event):
    """Emitted when a convention is created via deploy.

    Carries hook snapshots so downstream handlers (e.g. CreateFeatureTables)
    can create feature tables without querying the convention repository.
    """

    id: EventId
    convention_srn: ConventionSRN
    hooks: list[HookSnapshot] = []
