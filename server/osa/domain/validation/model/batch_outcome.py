"""Per-record outcome from a batch hook run."""

from typing import Any

from osa.domain.shared.model.value import ValueObject


class BatchRecordOutcome(ValueObject):
    """Per-record outcome from a batch hook execution.

    Each record in a batch ends up in exactly one of three states:
    passed (with features), rejected (with reason), or errored.
    """

    record_id: str
    status: str  # "passed", "rejected", "errored"
    features: list[dict[str, Any]] = []
    reason: str | None = None
    error: str | None = None
    retryable: bool = False
