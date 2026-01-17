from typing import Any, Generic, TypeVar

from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import DepositionSRN, RecordSRN

T = TypeVar("T")


class Deposition(Aggregate, Generic[T]):
    srn: DepositionSRN
    status: DepositionStatus
    metadata: T
    files: list[DepositionFile] = []
    record_srn: RecordSRN | None = None
    provenance: dict[str, Any] = {}  # Source info, provenance tracking

    def remove_all_files(self) -> None:
        self.files = []
