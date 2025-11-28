from typing import Optional, NewType

from osa.domain.shadow.model.value import ShadowStatus
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import DepositionSRN, DepositionProfileSRN


ShadowId = NewType("ShadowId", str)


class ShadowRequest(Aggregate):
    id: ShadowId
    status: ShadowStatus
    source_url: str
    profile_srn: DepositionProfileSRN
    deposition_id: Optional[DepositionSRN] = None
