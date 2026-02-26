"""SourceRecordReady event — emitted per record produced by a source container."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.srn import ConventionSRN


class SourceRecordReady(Event):
    """Emitted for each record produced by a source run.

    Replaces direct DepositionService calls in SourceService.
    Consumed by CreateDepositionFromSource in the deposition domain.
    """

    id: EventId
    convention_srn: ConventionSRN
    metadata: dict[str, Any]
    file_paths: list[str]
    source_id: str
    staging_dir: str
