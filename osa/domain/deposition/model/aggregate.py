from typing import Generic, Optional, TypeVar
from osa.domain.deposition.model.value import DepositionStatus
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.validation.model.aggregate import SemanticGuarantee

T = TypeVar("T")

# Thought: I feel like `Deposition` should not include `schema` or `guarantees`. Instead, the OSA should decide centrally how incoming submissions should be validated.
# i.e. it should offer "types" of deposition, from which the submitter must choose one.
# If the submitter can choose an arbitrary schema/validators, can they pick joke schemas and potentially dangerous validators?
class Deposition(Aggregate, Generic[T]):
    srn: DepositionSRN
    schema_id: str  # TODO: switch to SchemaId NewType
    guarantees: list[SemanticGuarantee]
    status: DepositionStatus
    payload: T
    record_id: Optional[str] = None  # TODO: switch to RecordId NewType
