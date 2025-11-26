from pydantic import Field
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import SRN, Semver
from osa.domain.validation.model.entity import GuaranteeValidator


class SemanticGuarantee(Aggregate):
    id: SRN
    semver: Semver
    _schema: str = Field(alias="schema")  # switch to SchemaId type, avoiding primitives
    validator: GuaranteeValidator
