"""Record aggregate - immutable published record."""

from datetime import datetime
from typing import Any

from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.source import RecordSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN


class Record(Aggregate):
    """An immutable, versioned, published record."""

    srn: RecordSRN
    source: RecordSource
    convention_srn: ConventionSRN
    metadata: dict[str, Any]
    published_at: datetime
