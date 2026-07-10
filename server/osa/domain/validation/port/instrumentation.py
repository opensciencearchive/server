"""HookInstrumentation port — a domain-probe for hook-execution telemetry.

One method per business fact worth measuring (a run finished; a failure was
decided). Implemented by infrastructure (an OTel adapter); trivially no-op-able
in tests. All methods are synchronous — metric emission must never block the
execution path — and keyword-only so call sites read as named facts, not
positional tuples. Labels are typed value objects / enums so metric cardinality
stays bounded and stringly-typed labels never leak into domain code.
"""

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.failure import DecisionKind, FailureKind
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.port import Port
from osa.domain.validation.model.hook_run import HookRunStatus


class HookInstrumentation(Port, Protocol):
    """Domain-probe for hook-execution metrics (see module docstring)."""

    @abstractmethod
    def run_finished(
        self, *, hook: HookName, status: HookRunStatus, duration_s: float, oom_retries: int
    ) -> None:
        """Record that one hook execution completed with a terminal status."""
        ...

    @abstractmethod
    def run_failure_decided(
        self, *, hook: HookName, kind: FailureKind, decision: DecisionKind
    ) -> None:
        """Record the policy decision taken for one observed hook failure."""
        ...
