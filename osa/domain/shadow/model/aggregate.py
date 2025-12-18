from typing import NewType, Optional

from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


ShadowId = NewType("ShadowId", str)


class ShadowRequest(Aggregate):
    id: ShadowId
    status: ShadowStatus
    source_url: str
    convention_srn: ConventionSRN
    deposition_id: Optional[DepositionSRN] = None
