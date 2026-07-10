"""OTel adapter implementing :class:`HookInstrumentation`.

Owns the ``osa_hook_*`` metric family — no other class emits these names. Label
values are drawn only from bounded value objects / enums (``HookName.root`` and
``StrEnum.value``) so time-series cardinality stays finite.
"""

from opentelemetry.metrics import Meter

from osa.domain.shared.failure import DecisionKind, FailureKind
from osa.domain.shared.model.hook import HookName
from osa.domain.validation.model.hook_run import HookRunStatus
from osa.domain.validation.port.instrumentation import HookInstrumentation


class OtelHookInstrumentation(HookInstrumentation):
    """Emits hook-execution metrics through an injected OTel :class:`Meter`."""

    def __init__(self, meter: Meter) -> None:
        self._runs = meter.create_counter(
            "osa_hook_runs_total",
            description="Count of completed hook executions, by hook and terminal status.",
        )
        self._duration = meter.create_histogram(
            "osa_hook_run_duration_seconds",
            unit="s",
            description="Wall-clock duration of hook executions, by hook and status.",
        )
        self._failures = meter.create_counter(
            "osa_hook_failures_total",
            description="Count of hook failures, by hook, observed cause, and policy decision.",
        )
        self._oom_retries = meter.create_counter(
            "osa_hook_oom_retries_total",
            description="Count of out-of-memory retries (memory bumps) performed, by hook.",
        )

    def run_finished(
        self, *, hook: HookName, status: HookRunStatus, duration_s: float, oom_retries: int
    ) -> None:
        attrs = {"hook": hook.root, "status": status.value}
        self._runs.add(1, attrs)
        self._duration.record(duration_s, attrs)
        if oom_retries > 0:
            self._oom_retries.add(oom_retries, {"hook": hook.root})

    def run_failure_decided(
        self, *, hook: HookName, kind: FailureKind, decision: DecisionKind
    ) -> None:
        self._failures.add(
            1,
            {"hook": hook.root, "kind": kind.value, "decision": decision.value},
        )
