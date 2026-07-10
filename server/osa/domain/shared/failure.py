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

from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import ClassVar, assert_never

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


class DecisionKind(StrEnum):
    """Bounded label vocabulary for the decision a :class:`FailurePolicy` picks.

    A metric-friendly projection of the :data:`Decision` union: one member per
    Decision variant. Keeps failure/decision metric labels algebraic (bounded
    cardinality) instead of stringly-typed at emission sites.
    """

    RETRY = "retry"
    RETRY_WITH_MORE_MEMORY = "retry_with_more_memory"
    GIVE_UP = "give_up"
    ABORT_RUN = "abort_run"


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

    kind_label: ClassVar[DecisionKind] = DecisionKind.RETRY


@dataclass(frozen=True)
class RetryWithMoreMemory:
    """Re-run with a doubled memory limit — the only adjust-and-rerun today."""

    kind_label: ClassVar[DecisionKind] = DecisionKind.RETRY_WITH_MORE_MEMORY


@dataclass(frozen=True)
class GiveUp:
    """Stop trying this unit of work (batch / pull); the run continues."""

    reason: str
    kind: FailureKind

    kind_label: ClassVar[DecisionKind] = DecisionKind.GIVE_UP


@dataclass(frozen=True)
class AbortRun:
    """The failure recurs identically for every batch — stop the whole run."""

    reason: str
    kind: FailureKind

    kind_label: ClassVar[DecisionKind] = DecisionKind.ABORT_RUN


Decision = Retry | RetryWithMoreMemory | GiveUp | AbortRun


def _precedence(decision: Decision) -> int:
    """Rank a decision by blast radius, so several can be reduced to the one that wins."""
    match decision:
        case AbortRun():
            return 3  # kills the whole run
        case Retry():
            return 2  # re-drive the batch
        case RetryWithMoreMemory():
            return 1  # remediation; terminal at batch level (bump lever is HookService's)
        case GiveUp():
            return 0  # record this unit and move on
    assert_never(decision)


def most_severe(decisions: Iterable[Decision]) -> Decision | None:
    """The decision that dominates when one batch yields several.

    When a batch runs N hooks, each failed hook produces a decision; the batch's
    fate is the most severe among them (:func:`_precedence`): an ``AbortRun``
    dominates a ``Retry`` dominates the terminal per-hook verbs. Lets a handler
    pick one action without a chain of ``isinstance`` checks. Returns ``None``
    for a batch that produced no decision (nothing failed); ties keep the
    first-seen decision.
    """
    return max(decisions, key=_precedence, default=None)


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
