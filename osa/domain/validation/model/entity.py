from datetime import datetime

from pydantic import Field

from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.srn import ValidationRunSRN
from osa.domain.shared.model.value import ValueObject
from osa.domain.validation.model.value import CheckStatus, RunStatus


class CheckResult(ValueObject):
    """Result of a single validation check."""

    check_id: str
    validator_digest: str
    status: CheckStatus
    message: str | None = None
    details: dict | None = None


class ValidationRun(Entity):
    """Execution of validation checks."""

    srn: ValidationRunSRN
    status: RunStatus = RunStatus.PENDING
    results: list[CheckResult] = Field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None
