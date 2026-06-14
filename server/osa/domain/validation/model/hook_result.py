"""Validation domain models for hook execution results."""

from enum import StrEnum

from pydantic import Field

from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.value import ValueObject


class HookStatus(StrEnum):
    PASSED = "passed"
    REJECTED = "rejected"


class ProgressEntry(ValueObject):
    """A single progress update from a hook."""

    step: str | None = None
    status: str
    message: str | None = None


class HookResult(ValueObject):
    """Result of executing a single hook."""

    hook_name: HookName
    status: HookStatus
    rejection_reason: str | None = None
    error_message: str | None = None
    progress: list[ProgressEntry] = Field(default_factory=list)
    duration_seconds: float
    oom_retries: int = 0
    """Number of times the run was retried with doubled memory after an OOM
    eviction (#145). 0 for a clean single-attempt run. Surfaced into the
    ``hook_runs`` provenance record."""
