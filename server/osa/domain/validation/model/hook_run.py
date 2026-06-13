"""HookRun — append-only execution record + per-row provenance anchor (#145).

One row per (execution context, batch, hook). Points at the snapshotted
:class:`HookRelease` that ran, and is the FK target stamped onto every feature
row (``features.*.run_id``). Carries status/timing/logs. Exactly one execution
context (ingest run *or* deposition) is set.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import NewType
from uuid import UUID

from pydantic import model_validator

from osa.domain.ingest.model.ingest_run import IngestRunId
from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.validation.model.hook_release import HookReleaseId

HookRunId = NewType("HookRunId", UUID)


class HookRunStatus(StrEnum):
    PASSED = "passed"
    WARNINGS = "warnings"
    FAILED = "failed"
    ERROR = "error"


class HookRun(Entity):
    """Append-only execution record; provenance + logs anchor."""

    id: HookRunId
    release_id: HookReleaseId
    ingest_run_id: IngestRunId | None = None
    deposition_id: DepositionSRN | None = None
    batch_index: int | None = None
    status: HookRunStatus
    started_at: datetime
    finished_at: datetime | None = None
    duration_s: float | None = None
    oom_retries: int = 0
    log_ref: str | None = None

    @model_validator(mode="after")
    def _exactly_one_context(self) -> "HookRun":
        has_ingest = self.ingest_run_id is not None
        has_deposition = self.deposition_id is not None
        if has_ingest == has_deposition:
            raise ValueError(
                "HookRun requires exactly one execution context (ingest_run_id XOR deposition_id)"
            )
        return self
