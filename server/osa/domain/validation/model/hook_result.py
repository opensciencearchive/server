"""Validation domain models for hook execution results."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import Field

from osa.domain.shared.failure import FailureKind, RuntimeFailure
from osa.domain.shared.model.hook import HookIdentity, HookName
from osa.domain.shared.model.value import ValueObject
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId


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


class HookExecution(ValueObject):
    """One hook's **total** outcome from a batch run, with its own wall-clock window (#145).

    Every hook produces exactly one of these — passed, rejected, or errored
    (with the observed :class:`FailureKind`) — instead of a failure unwinding
    the batch loop and discarding sibling hooks' results. The kind is a fact
    (what happened); what to do about it is the FailurePolicy's call in the
    handler. ``started_at``/``finished_at`` bracket *this* hook's run (hooks run
    sequentially, so a shared batch window would misdate every hook after the
    first).
    """

    hook_name: HookName
    release_id: HookReleaseId
    status: HookStatus | None  # None when the hook errored (failure is set instead)
    started_at: datetime
    finished_at: datetime
    duration_s: float
    oom_retries: int = 0
    failure: FailureKind | None = None
    error_message: str | None = None
    log_text: str | None = None
    """Captured stdout/stderr of the failed container, when the runner grabbed it
    (#145/#147). The ingestion handler persists this to a tenant-scoped artifact
    and records the locator as ``HookRun.log_ref``. ``None`` for passed hooks or
    when log capture itself failed."""

    @classmethod
    def completed(
        cls,
        hook: HookIdentity,
        release: HookRelease,
        result: HookResult,
        started_at: datetime,
        finished_at: datetime,
    ) -> HookExecution:
        return cls(
            hook_name=hook.name,
            release_id=release.id,
            status=result.status,
            started_at=started_at,
            finished_at=finished_at,
            duration_s=result.duration_seconds,
            oom_retries=result.oom_retries,
            failure=None,
            error_message=result.error_message or result.rejection_reason,
        )

    @classmethod
    def failed(
        cls,
        hook: HookIdentity,
        release: HookRelease,
        failure: RuntimeFailure,
        started_at: datetime,
        finished_at: datetime,
    ) -> HookExecution:
        # The failure already carries the observed facts — no isinstance tree.
        return cls(
            hook_name=hook.name,
            release_id=release.id,
            status=None,
            started_at=started_at,
            finished_at=finished_at,
            duration_s=(finished_at - started_at).total_seconds(),
            oom_retries=failure.oom_retries,
            failure=failure.kind,
            error_message=str(failure),
            log_text=failure.container_logs,
        )
