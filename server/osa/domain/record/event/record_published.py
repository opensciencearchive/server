"""RecordPublished event - emitted when a record is published and ready for indexing."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN, RecordSRN


class RecordPublished(Event):
    """Emitted when a record is published and ready for indexing.

    Enriched with convention_srn, hooks, and files_dir so downstream
    consumers (feature insertion, indexing) can operate without
    querying deposition/convention repositories.
    """

    id: EventId
    record_srn: RecordSRN
    deposition_srn: DepositionSRN
    metadata: dict[str, Any]
    convention_srn: ConventionSRN | None = None
    hooks: list[HookSnapshot] = []
    files_dir: str = ""
