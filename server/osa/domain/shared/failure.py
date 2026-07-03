"""Runtime failure taxonomy: facts → policy → action (#152).

When a hook or ingester container fails, three concerns must stay apart:

- **Facts** (:class:`RuntimeFailure`): what the runner observed — the cause
  (:class:`FailureKind`) plus diagnostics. No disposition baked in.
- **Policy** (:class:`FailurePolicy`): one pure function mapping
  ``(failure, prior attempts) → action``. The only place the rules live.
- **Actions** (:data:`Decision`): the verb an executor carries out. Blast
  radius is *which* verb the policy picks (:class:`GiveUp` dooms one batch,
  :class:`AbortRun` dooms the whole run) — not a property failures carry.

Budget split: the outbox/worker keeps the generic transient redelivery budget
(it owns ``delivery.retry_count``), so :class:`Retry` is unconditional here and
exhaustion is enforced by the worker via ``on_exhausted``. The policy owns the
one budget the outbox cannot see: OOM memory bumps (:class:`PriorAttempts`).

These exceptions drive worker retry policy, not HTTP responses, so
:class:`RuntimeFailure` deliberately sits outside the 503-mapped
``InfrastructureError`` tree.
"""

from dataclasses import dataclass
from enum import StrEnum

from osa.domain.shared.error import OSAError


class FailureKind(StrEnum):
    """The observed cause of a hook/ingester runtime failure."""

    IMAGE_PULL = "image_pull"  # ErrImagePull / ImagePullBackOff
    RBAC = "rbac"  # K8s API 403 — ServiceAccount misconfiguration
    CONFIG = "config"  # environment misconfiguration (missing namespace, ...)
    OOM = "oom"  # container killed by the OOM killer
    TIMEOUT = "timeout"  # scheduling/execution/watch deadline exceeded
    UPSTREAM = "upstream"  # ingester non-zero exit — usually an upstream API failure
    HOOK_EXIT = "hook_exit"  # hook non-zero exit — deterministic code failure
    RUNTIME = "runtime"  # container runtime hiccup (docker error, eviction, K8s 5xx)
    UNKNOWN = "unknown"


class RuntimeFailure(OSAError):
    """A runtime failure observation raised by a container runner.

    Facts only — the cause and diagnostics of what happened. Disposition
    (retry / give up / abort) is decided by :class:`FailurePolicy`, never here.
    """

    def __init__(
        self,
        kind: FailureKind,
        detail: str,
        *,
        exit_code: int | None = None,
        container_logs: str | None = None,
        oom_retries: int = 0,
    ) -> None:
        super().__init__(detail, code=kind.value)
        self.kind = kind
        self.detail = detail
        self.exit_code = exit_code
        # Captured stdout/stderr of the failed container, when available. Some
        # runners fetch logs only in their catch block and assign post-raise.
        self.container_logs = container_logs
        # Memory bumps performed before HookService gave up on an OOM (#145).
        # Runners always raise with 0; HookService re-raises with the real count.
        self.oom_retries = oom_retries


@dataclass(frozen=True)
class PriorAttempts:
    """Remediation state the policy consults — a view over existing data.

    Only the budget the outbox doesn't track: transient redelivery counts stay
    on ``deliveries.retry_count`` and are enforced by the worker.
    """

    memory_bumps: int


@dataclass(frozen=True)
class Retry:
    """Re-drive the failed unit of work; the worker's delivery budget bounds it."""


@dataclass(frozen=True)
class RetryWithMoreMemory:
    """Re-run with a doubled memory limit — the only adjust-and-rerun today."""


@dataclass(frozen=True)
class GiveUp:
    """Stop trying this unit of work (batch / pull); the run continues."""

    reason: str
    kind: FailureKind


@dataclass(frozen=True)
class AbortRun:
    """The failure recurs identically for every batch — stop the whole run."""

    reason: str
    kind: FailureKind


Decision = Retry | RetryWithMoreMemory | GiveUp | AbortRun


@dataclass(frozen=True)
class FailurePolicy:
    """The whole runtime-failure decision matrix, as one pure function."""

    max_memory_bumps: int = 3

    def decide(self, failure: RuntimeFailure, attempts: PriorAttempts) -> Decision:
        """Map an observed failure + prior remediation attempts to an action."""
        match failure.kind:
            case FailureKind.IMAGE_PULL | FailureKind.RBAC | FailureKind.CONFIG:
                # Environmental — will recur identically; nothing to retry.
                return AbortRun(reason=failure.detail, kind=failure.kind)
            case FailureKind.OOM:
                if attempts.memory_bumps < self.max_memory_bumps:
                    return RetryWithMoreMemory()
                return GiveUp(
                    reason=f"out of memory headroom after {attempts.memory_bumps} bumps",
                    kind=failure.kind,
                )
            case FailureKind.TIMEOUT | FailureKind.UPSTREAM | FailureKind.RUNTIME:
                return Retry()
            case FailureKind.HOOK_EXIT:
                # Deterministic code failure — a blind rerun would fail identically.
                return GiveUp(
                    reason=f"hook exited with code {failure.exit_code}", kind=failure.kind
                )
            case _:
                return GiveUp(reason=failure.detail, kind=failure.kind)
