"""OutboxInstrumentation port — a domain-probe for outbox-delivery telemetry.

One method per business fact worth measuring (a delivery reached a terminal
disposition). Consumed by the infrastructure worker and implemented by an OTel
adapter; trivially no-op-able in tests. Synchronous (emission must never block
dispatch) and keyword-only. Labels are typed enums so cardinality stays bounded.
"""

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.event import DeliveryStatus
from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage
from osa.domain.shared.port import Port


class OutboxInstrumentation(Port, Protocol):
    """Domain-probe for outbox-delivery metrics (see module docstring)."""

    @abstractmethod
    def delivery_completed(
        self,
        *,
        consumer_group: str,
        status: DeliveryStatus,
        retry_count: int,
        duration_s: float,
    ) -> None:
        """Record a delivery reaching a terminal disposition after dispatch."""
        ...


class WorkflowInstrumentation(Port, Protocol):
    """Domain-probe for workflow-stage outcomes.

    Emitted from the orchestrator stage guards. The retry-safety invariant of
    #160 ("a retry must not re-run containers") becomes directly alertable: an
    ``outcome=ran`` where the guard was expected to report ``skipped`` on a
    redelivery means a stage checkpoint was lost and work was repeated.
    """

    @abstractmethod
    def stage_finished(
        self, *, workflow: WorkflowName, stage: WorkflowStage, outcome: StageOutcome
    ) -> None:
        """Record a workflow stage concluding with the given outcome."""
        ...
