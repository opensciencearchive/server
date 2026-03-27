"""IngesterRecord — typed representation of a record from an ingester container."""

from typing import Any

from osa.domain.shared.model.value import ValueObject


class IngesterRecord(ValueObject):
    """A record produced by an ingester container, parsed from records.jsonl.

    Replaces raw dicts with typed fields so downstream handlers
    don't need fragile `.get("source_id", .get("id", ""))` patterns.
    """

    source_id: str
    metadata: dict[str, Any]
    file_paths: list[str] = []
