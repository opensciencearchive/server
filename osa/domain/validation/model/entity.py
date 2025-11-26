from datetime import datetime

from pydantic import Field
from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.srn import SRN
from osa.domain.validation.model.value import ValidationStatus, ValidatorMessage


class GuaranteeValidator(Entity):
    id: SRN
    image: str  # switch to Uri or OCIImage, created in shared/models


class ValidationCheck(Entity):
    status: ValidationStatus
    message: ValidatorMessage


class ValidationSummary(Entity):
    status: ValidationStatus
    checks: list[ValidationCheck]
    generated_at: datetime = Field(default_factory=datetime.now)
