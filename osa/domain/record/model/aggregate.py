"""Record aggregate - immutable published record."""

from datetime import datetime
from typing import Any

from osa.domain.record.model.value import IndexRef
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN


class Record(Aggregate):
    """An immutable, versioned, published record."""

    srn: RecordSRN
    deposition_srn: DepositionSRN
    metadata: dict[str, Any]
    indexes: dict[str, IndexRef] = {}
    published_at: datetime
