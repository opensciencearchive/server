"""OTel adapter implementing :class:`WorkflowInstrumentation`.

Owns the ``osa_workflow_stages_total`` metric — no other class emits this name.
Label values are drawn only from bounded ``StrEnum`` members (``.value``) so
time-series cardinality stays finite (``WorkflowName × WorkflowStage ×
StageOutcome``).
"""

from opentelemetry.metrics import Meter

from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage
from osa.domain.shared.port.instrumentation import WorkflowInstrumentation


class OtelWorkflowInstrumentation(WorkflowInstrumentation):
    """Emits workflow-stage metrics through an injected OTel :class:`Meter`."""

    def __init__(self, meter: Meter) -> None:
        self._stages = meter.create_counter(
            "osa_workflow_stages_total",
            description="Workflow stage executions by outcome.",
        )

    def stage_finished(
        self, *, workflow: WorkflowName, stage: WorkflowStage, outcome: StageOutcome
    ) -> None:
        self._stages.add(
            1,
            {"workflow": workflow.value, "stage": stage.value, "outcome": outcome.value},
        )
