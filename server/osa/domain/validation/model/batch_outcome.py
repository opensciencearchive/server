"""Per-record outcome from a batch hook run."""

from enum import StrEnum
from typing import Any, NewType

from osa.domain.shared.model.value import ValueObject

HookRecordId = NewType("HookRecordId", str)


class OutcomeStatus(StrEnum):
    """Outcome status for a single record in a batch hook execution."""

    PASSED = "passed"
    REJECTED = "rejected"
    ERRORED = "errored"


class BatchRecordOutcome(ValueObject):
    """Per-record outcome from a batch hook execution.

    Each record in a batch ends up in exactly one of three states:
    passed (with features), rejected (with reason), or errored.
    """

    record_id: HookRecordId
    status: OutcomeStatus
    features: list[dict[str, Any]] = []
    reason: str | None = None
    error: str | None = None
    retryable: bool = False
