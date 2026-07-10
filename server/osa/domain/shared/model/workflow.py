"""Bounded label vocabulary for workflow-stage metrics (#160).

A single stage set is shared by both orchestrated workflows so metric
cardinality stays fixed: every ``osa_workflow_stages_total`` time series is a
member of ``WorkflowName × WorkflowStage × StageOutcome``. Not every workflow
visits every stage — the shared enum keeps the label space closed regardless.
"""

from enum import StrEnum


class WorkflowName(StrEnum):
    """The orchestrated workflows that emit stage metrics."""

    PROCESS_SUBMISSION = "process_submission"
    PROCESS_BATCH = "process_batch"


class WorkflowStage(StrEnum):
    """The stages a workflow may pass through (shared across workflows)."""

    VALIDATE = "validate"
    CURATE = "curate"
    INGEST = "ingest"
    HOOKS = "hooks"
    PUBLISH = "publish"
    INSERT_FEATURES = "insert_features"


class StageOutcome(StrEnum):
    """How a stage concluded on a given delivery attempt."""

    RAN = "ran"
    SKIPPED = "skipped"
    FAILED = "failed"
