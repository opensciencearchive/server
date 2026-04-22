"""RecordPublished event - emitted when a record is published and ready for indexing."""

from typing import Any

from osa.domain.shared.event import Event, EventId
from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN, SchemaId


class RecordPublished(Event):
    """Emitted when a record is published and ready for indexing.

    Carries ``schema_id`` so downstream consumers (metadata insertion,
    indexing) operate in terms of short-form identity rather than full URNs.
    """

    id: EventId
    record_srn: RecordSRN
    source: RecordSource
    convention_srn: ConventionSRN
    schema_id: SchemaId
    metadata: dict[str, Any]
    expected_features: list[str] = []
