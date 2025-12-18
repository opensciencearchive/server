from datetime import datetime

from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.srn import TraitSRN
from osa.domain.validation.model.value import TraitStatus, Validator


class Trait(Aggregate):
    """A verifiable assertion about data, coupled 1:1 with its OCI validator."""

    srn: TraitSRN
    slug: str
    name: str
    description: str
    validator: Validator
    status: TraitStatus = TraitStatus.DRAFT
    created_at: datetime
