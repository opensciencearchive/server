from datetime import datetime

from pydantic import Field

from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.srn import TraitSRN, ValidationRunSRN
from osa.domain.shared.model.value import ValueObject
from osa.domain.validation.model.value import CheckStatus, RunStatus


class CheckResult(ValueObject):
    """Result of validating a single trait."""

    trait_srn: str
    validator_digest: str
    status: CheckStatus
    message: str | None = None
    details: dict | None = None


class ValidationRun(Entity):
    """Execution of validation against a set of traits."""

    srn: ValidationRunSRN
    trait_srns: list[TraitSRN] = Field(default_factory=list)
    status: RunStatus = RunStatus.PENDING
    results: list[CheckResult] = Field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None
