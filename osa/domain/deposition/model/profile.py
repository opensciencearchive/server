from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import (
    DepositionProfileSRN,
    GuaranteeSRN,
    SchemaSRN,
)


class DepositionProfile(Aggregate):
    srn: DepositionProfileSRN
    schema_srn: SchemaSRN
    guarantee_srns: list[GuaranteeSRN]
