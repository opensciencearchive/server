from datetime import datetime

from pydantic import Field

from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.srn import ValidationRunSRN
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.model.value import RunStatus


class ValidationRun(Entity):
    """Execution of validation hooks."""

    srn: ValidationRunSRN
    status: RunStatus = RunStatus.PENDING
    results: list[HookResult] = Field(default_factory=list)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    expires_at: datetime | None = None

    @property
    def summary(self) -> HookStatus | None:
        """Overall hook result summary."""
        if not self.results:
            return None
        statuses = [r.status for r in self.results]
        if HookStatus.FAILED in statuses:
            return HookStatus.FAILED
        if HookStatus.REJECTED in statuses:
            return HookStatus.REJECTED
        return HookStatus.PASSED
