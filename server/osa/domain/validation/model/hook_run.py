"""HookRun — pure execution record + per-row provenance anchor (#145).

One row per (hook execution at completion). Points at the snapshotted
:class:`HookRelease` that ran, and is the FK target stamped onto every feature
row (``features.*.run_id``). Carries only status/timing/logs — *what code ran,
when, and where the logs are*.

Where the input data came from is **not** restated here: a feature row reaches
its origin via the other arm of the join, ``record_srn → records.source``. The
old execution-context columns (``ingest_run_id`` / ``deposition_id`` /
``batch_index`` and the XOR check) existed only to key an insert-time run-id
lookup that has been removed in favour of carrying ``run_id`` through the hook
output dir (``run.json``).
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import NewType
from uuid import UUID

from osa.domain.shared.model.entity import Entity
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.model.hook_result import HookStatus

HookRunId = NewType("HookRunId", UUID)


class HookRunStatus(StrEnum):
    PASSED = "passed"
    WARNINGS = "warnings"
    FAILED = "failed"
    ERROR = "error"

    @classmethod
    def from_hook_status(cls, status: HookStatus) -> "HookRunStatus":
        """Map a per-hook execution outcome to its append-only run status.

        Total over :class:`HookStatus` (``PASSED`` / ``REJECTED``). The
        "no result / errored" case has no ``HookStatus`` to map from — it is
        handled explicitly at the single call site that has it (→ ``ERROR``).
        """
        if status == HookStatus.PASSED:
            return cls.PASSED
        return cls.FAILED


class HookRun(Entity):
    """Append-only execution record; provenance + logs anchor.

    Runs are recorded as a single insert at completion, so ``finished_at`` /
    ``duration_s`` / ``oom_retries`` are always known. ``log_ref`` is the only
    genuine optional (logs may not have been persisted).
    """

    id: HookRunId
    release_id: HookReleaseId
    status: HookRunStatus
    started_at: datetime
    finished_at: datetime
    duration_s: float
    oom_retries: int
    log_ref: str | None = None
