"""StageRunner — spans + outcome emission around workflow stages (#160)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import logfire

from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage
from osa.domain.shared.port.instrumentation import WorkflowInstrumentation
from osa.infrastructure.logging import get_logger

logger = get_logger(__name__)


class StageRunner:
    """Wraps workflow stages in a span + outcome emission (#160)."""

    def __init__(self, workflow: WorkflowName, instrumentation: WorkflowInstrumentation) -> None:
        self._workflow = workflow
        self._instrumentation = instrumentation

    @asynccontextmanager
    async def run(self, stage: WorkflowStage) -> AsyncIterator[None]:
        """Run a stage inside a span; emit RAN on clean exit, FAILED on error.

        Any exception raised in the body emits a FAILED outcome and re-raises so
        the caller's retry machinery still sees it.
        """
        with logfire.span(
            "workflow.stage {stage}", workflow=self._workflow.value, stage=stage.value
        ):
            try:
                yield
            except BaseException:
                self._instrumentation.stage_finished(
                    workflow=self._workflow, stage=stage, outcome=StageOutcome.FAILED
                )
                raise
            self._instrumentation.stage_finished(
                workflow=self._workflow, stage=stage, outcome=StageOutcome.RAN
            )

    def skipped(self, stage: WorkflowStage) -> None:
        """Record that a stage was skipped because its work is already complete."""
        logger.info(
            "stage skipped: already complete",
            workflow=self._workflow.value,
            stage=stage.value,
        )
        self._instrumentation.stage_finished(
            workflow=self._workflow, stage=stage, outcome=StageOutcome.SKIPPED
        )
