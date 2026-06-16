"""Record aggregate - immutable published record."""

from datetime import datetime
from typing import Any

from pydantic import Field

from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSlug, RecordSRN, SchemaId


class Record(Aggregate):
    """An immutable, versioned, published record."""

    srn: RecordSRN
    source: RecordSource
    convention_id: ConventionSlug
    schema_id: SchemaId = Field(frozen=True)
    metadata: dict[str, Any]
    published_at: datetime
