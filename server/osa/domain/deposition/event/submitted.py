from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.srn import ConventionId, DepositionSRN


class DepositionSubmittedEvent(Event):
    """Emitted when a deposition is submitted for validation.

    Enriched with convention_id and hook **names** so the validation domain can
    operate without querying deposition repos. The validation handler resolves
    each name's live release at run start (feature #145, R8) — never re-freezing
    a digest into the event.
    """

    id: EventId
    deposition_id: DepositionSRN
    metadata: dict[str, Any]
    convention_id: ConventionId
    hooks: list[HookName] = []
