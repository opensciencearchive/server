"""Value objects for hook input data."""

from typing import Any

from osa.domain.shared.model.value import ValueObject


class HookRecord(ValueObject):
    """A single record to be processed by a hook.

    Maps to one line in records.jsonl: {"id": "...", "metadata": {...}}.
    """

    id: str
    metadata: dict[str, Any]
    size_hint_mb: float = 0
