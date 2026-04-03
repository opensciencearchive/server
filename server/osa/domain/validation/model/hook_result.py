"""Validation domain models for hook execution results."""

from enum import StrEnum

from pydantic import Field

from osa.domain.shared.model.value import ValueObject


class HookStatus(StrEnum):
    PASSED = "passed"
    REJECTED = "rejected"
    FAILED = "failed"
    OOM = "oom"


class ProgressEntry(ValueObject):
    """A single progress update from a hook."""

    step: str | None = None
    status: str
    message: str | None = None


class HookResult(ValueObject):
    """Result of executing a single hook."""

    hook_name: str
    status: HookStatus
    rejection_reason: str | None = None
    error_message: str | None = None
    progress: list[ProgressEntry] = Field(default_factory=list)
    duration_seconds: float

    @property
    def oom_killed(self) -> bool:
        """Whether this hook was killed by an out-of-memory condition."""
        return self.status == HookStatus.OOM
