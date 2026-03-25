"""RecordPublished event - emitted when a record is published and ready for indexing."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN


class RecordPublished(Event):
    """Emitted when a record is published and ready for indexing.

    Enriched with source, convention_srn, and expected_features so downstream
    consumers (feature insertion, indexing) can operate without querying
    record/convention repositories.
    """

    id: EventId
    record_srn: RecordSRN
    source: RecordSource
    convention_srn: ConventionSRN
    metadata: dict[str, Any]
    expected_features: list[str] = []
