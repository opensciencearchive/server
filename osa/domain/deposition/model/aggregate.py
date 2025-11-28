from typing import Generic, Optional, TypeVar
from osa.domain.deposition.model.value import DepositionStatus, DepositionFile
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import DepositionSRN, DepositionProfileSRN

T = TypeVar("T")


class Deposition(Aggregate, Generic[T]):
    srn: DepositionSRN
    profile_srn: DepositionProfileSRN
    status: DepositionStatus
    payload: T
    files: list[DepositionFile] = []
    record_id: Optional[str] = None  # TODO: switch to RecordId NewType

    def remove_all_files(self) -> None:
        self.files = []
