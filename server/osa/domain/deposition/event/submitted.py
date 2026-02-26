from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


class DepositionSubmittedEvent(Event):
    """Emitted when a deposition is submitted for validation.

    Enriched with convention_srn, hooks, and files_dir so the
    validation domain can operate without querying deposition repos.
    """

    id: EventId
    deposition_id: DepositionSRN
    metadata: dict[str, Any]
    convention_srn: ConventionSRN
    hooks: list[HookSnapshot] = []
    files_dir: str = ""
