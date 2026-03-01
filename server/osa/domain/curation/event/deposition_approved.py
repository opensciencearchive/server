"""DepositionApproved event - emitted when a deposition passes curation."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


class DepositionApproved(Event):
    """Emitted when a deposition is approved for publication.

    Enriched with convention data so downstream consumers
    can operate without querying deposition repos.
    """

    id: EventId
    deposition_srn: DepositionSRN
    metadata: dict[str, Any]
    convention_srn: ConventionSRN | None = None
    hooks: list[HookSnapshot] = []
    files_dir: str = ""
