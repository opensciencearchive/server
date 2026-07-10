"""Unit tests for :class:`StageRunner`.

The runner wraps a workflow stage in a span and emits exactly one stage
outcome: RAN on clean exit, FAILED (and re-raise) on any exception, SKIPPED via
the explicit :meth:`StageRunner.skipped` call. A small recording fake stands in
for the instrumentation port.
"""

from dataclasses import dataclass, field

import pytest

from osa.application.workflow.stages import StageRunner
from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage


@dataclass
class _RecordingInstrumentation:
    """Records every ``stage_finished`` call for assertion."""

    calls: list[tuple[WorkflowName, WorkflowStage, StageOutcome]] = field(default_factory=list)

    def stage_finished(
        self, *, workflow: WorkflowName, stage: WorkflowStage, outcome: StageOutcome
    ) -> None:
        self.calls.append((workflow, stage, outcome))


async def test_run_emits_ran_on_clean_exit() -> None:
    instr = _RecordingInstrumentation()
    runner = StageRunner(WorkflowName.PROCESS_SUBMISSION, instr)

    async with runner.run(WorkflowStage.VALIDATE):
        pass

    assert instr.calls == [
        (WorkflowName.PROCESS_SUBMISSION, WorkflowStage.VALIDATE, StageOutcome.RAN)
    ]


async def test_run_emits_failed_and_reraises_on_exception() -> None:
    instr = _RecordingInstrumentation()
    runner = StageRunner(WorkflowName.PROCESS_BATCH, instr)

    sentinel = RuntimeError("boom")
    with pytest.raises(RuntimeError) as excinfo:
        async with runner.run(WorkflowStage.HOOKS):
            raise sentinel

    assert excinfo.value is sentinel
    assert instr.calls == [(WorkflowName.PROCESS_BATCH, WorkflowStage.HOOKS, StageOutcome.FAILED)]


def test_skipped_emits_skipped_outcome() -> None:
    instr = _RecordingInstrumentation()
    runner = StageRunner(WorkflowName.PROCESS_BATCH, instr)

    runner.skipped(WorkflowStage.INGEST)

    assert instr.calls == [(WorkflowName.PROCESS_BATCH, WorkflowStage.INGEST, StageOutcome.SKIPPED)]
