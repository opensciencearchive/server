from typing import Generic, Optional, TypeVar

from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import (
    ConventionSRN,
    DepositionSRN,
    RecordSRN,
    SchemaSRN,
    TraitSRN,
    VocabSRN,
)

T = TypeVar("T")


class Deposition(Aggregate, Generic[T]):
    srn: DepositionSRN
    convention_srn: ConventionSRN
    status: DepositionStatus
    payload: T
    files: list[DepositionFile] = []
    record_srn: Optional[RecordSRN] = None

    def remove_all_files(self) -> None:
        self.files = []


class Convention(Aggregate):
    """Bundle of schema + traits + vocabularies for submission."""

    srn: ConventionSRN
    schema_srn: SchemaSRN
    trait_srns: list[TraitSRN]
    vocab_srns: list[VocabSRN] = []
