"""FailurePolicy decision matrix — facts → policy → action (#152).

The policy is the ONE place runtime-failure disposition lives. Runners report
facts (RuntimeFailure with a cause FailureKind); the policy maps
(failure, prior attempts) → an action verb; executors carry it out.

Budget split (the crux from #152): the outbox/worker keeps the generic
transient redelivery budget (it owns delivery.retry_count), so the policy's
Retry is unconditional for retryable kinds. The policy owns the one budget the
outbox can't see: OOM memory bumps.
"""

import pytest

from osa.domain.shared.failure import (
    AbortRun,
    FailureKind,
    FailurePolicy,
    GiveUp,
    PriorAttempts,
    Retry,
    RetryWithMoreMemory,
    RuntimeFailure,
    most_severe,
)

FRESH = PriorAttempts(memory_bumps=0)


def _policy(max_memory_bumps: int = 3) -> FailurePolicy:
    return FailurePolicy(max_memory_bumps=max_memory_bumps)


class TestEnvironmentalFailuresAbortTheRun:
    """A failure that recurs identically for every batch dooms the whole run."""

    @pytest.mark.parametrize("kind", [FailureKind.IMAGE_PULL, FailureKind.RBAC, FailureKind.CONFIG])
    def test_aborts_run(self, kind: FailureKind) -> None:
        failure = RuntimeFailure(kind, "Image pull failed: 401 Unauthorized")

        decision = _policy().decide(failure, FRESH)

        assert decision == AbortRun(reason="Image pull failed: 401 Unauthorized", kind=kind)

    def test_aborts_even_with_prior_attempts(self) -> None:
        failure = RuntimeFailure(FailureKind.RBAC, "K8s RBAC permission denied")

        decision = _policy().decide(failure, PriorAttempts(memory_bumps=2))

        assert isinstance(decision, AbortRun)


class TestOomIsRemediatedThenGivenUp:
    """OOM retries with more memory until the bump budget is spent — no
    'permanent error secretly intercepted' any more."""

    def test_first_oom_gets_a_memory_bump(self) -> None:
        failure = RuntimeFailure(FailureKind.OOM, "Hook killed by OOM (limit: 512m)")

        decision = _policy(max_memory_bumps=3).decide(failure, FRESH)

        assert decision == RetryWithMoreMemory()

    def test_bump_below_budget_still_retries(self) -> None:
        failure = RuntimeFailure(FailureKind.OOM, "Hook killed by OOM (limit: 2g)")

        decision = _policy(max_memory_bumps=3).decide(failure, PriorAttempts(memory_bumps=2))

        assert decision == RetryWithMoreMemory()

    def test_budget_exhausted_gives_up(self) -> None:
        failure = RuntimeFailure(FailureKind.OOM, "Hook killed by OOM (limit: 4g)")

        decision = _policy(max_memory_bumps=3).decide(failure, PriorAttempts(memory_bumps=3))

        assert decision == GiveUp(
            reason="out of memory headroom after 3 bumps", kind=FailureKind.OOM
        )


class TestRetryableKindsDeferToTheOutboxBudget:
    """Timeout / upstream / runtime hiccups retry; exhaustion is enforced by
    the worker's delivery budget, not duplicated here."""

    @pytest.mark.parametrize(
        "kind", [FailureKind.TIMEOUT, FailureKind.UPSTREAM, FailureKind.RUNTIME]
    )
    def test_retries(self, kind: FailureKind) -> None:
        failure = RuntimeFailure(kind, "watch timeout waiting for Job")

        decision = _policy().decide(failure, FRESH)

        assert decision == Retry()


class TestDeterministicExitsAreNotBlindlyRetried:
    def test_hook_exit_gives_up_with_exit_code(self) -> None:
        failure = RuntimeFailure(FailureKind.HOOK_EXIT, "Hook exited with code 2", exit_code=2)

        decision = _policy().decide(failure, FRESH)

        assert decision == GiveUp(reason="hook exited with code 2", kind=FailureKind.HOOK_EXIT)

    def test_unknown_gives_up_with_detail(self) -> None:
        failure = RuntimeFailure(FailureKind.UNKNOWN, "Hook failed: PodFailurePolicy")

        decision = _policy().decide(failure, FRESH)

        assert decision == GiveUp(reason="Hook failed: PodFailurePolicy", kind=FailureKind.UNKNOWN)


class TestRuntimeFailureFacts:
    """RuntimeFailure is an observation: cause + diagnostics, no disposition."""

    def test_carries_typed_fields(self) -> None:
        failure = RuntimeFailure(
            FailureKind.HOOK_EXIT,
            "Hook exited with code 137",
            exit_code=137,
            container_logs="Traceback ...",
        )

        assert failure.kind is FailureKind.HOOK_EXIT
        assert failure.detail == "Hook exited with code 137"
        assert failure.exit_code == 137
        assert failure.container_logs == "Traceback ..."
        assert failure.oom_retries == 0
        assert str(failure) == "Hook exited with code 137"

    def test_is_not_an_http_mapped_infrastructure_error(self) -> None:
        # Runtime failures drive worker policy, not HTTP responses — they must
        # not inherit the 503-mapped InfrastructureError semantics.
        from osa.domain.shared.error import InfrastructureError, OSAError

        failure = RuntimeFailure(FailureKind.RUNTIME, "docker hiccup")

        assert isinstance(failure, OSAError)
        assert not isinstance(failure, InfrastructureError)


class TestMostSevere:
    """most_severe reduces one batch's per-hook decisions to the verb that wins.

    Precedence follows blast radius: AbortRun (kills the run) dominates Retry
    (re-drive the batch) dominates the terminal per-hook verbs. This is what lets
    a batch handler pick a single action without a chain of isinstance checks.
    """

    _GIVE = GiveUp(reason="hook exited 2", kind=FailureKind.HOOK_EXIT)
    _ABORT = AbortRun(reason="bad image", kind=FailureKind.IMAGE_PULL)

    def test_empty_batch_yields_none(self) -> None:
        assert most_severe([]) is None

    def test_single_decision_passes_through(self) -> None:
        assert most_severe([self._GIVE]) is self._GIVE

    def test_retry_dominates_give_up(self) -> None:
        assert most_severe([self._GIVE, Retry()]) == Retry()

    def test_retry_with_more_memory_ranks_above_give_up(self) -> None:
        assert most_severe([self._GIVE, RetryWithMoreMemory()]) == RetryWithMoreMemory()

    def test_abort_dominates_everything(self) -> None:
        assert most_severe([Retry(), self._GIVE, self._ABORT]) == self._ABORT

    def test_order_independent(self) -> None:
        assert most_severe([self._ABORT, Retry(), self._GIVE]) == self._ABORT

    def test_ties_keep_the_first_seen(self) -> None:
        first = AbortRun(reason="first", kind=FailureKind.RBAC)
        second = AbortRun(reason="second", kind=FailureKind.IMAGE_PULL)
        assert most_severe([first, second]) is first
