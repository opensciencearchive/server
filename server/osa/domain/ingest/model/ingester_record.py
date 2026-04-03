"""IngesterRecord — typed representation of a record from an ingester container."""

import json
import logging
from pathlib import Path
from typing import Any

from osa.domain.shared.model.value import ValueObject

logger = logging.getLogger(__name__)


class IngesterFileRef(ValueObject):
    """A reference to a file produced by an ingester container."""

    name: str
    relative_path: str
    size_mb: float


class IngesterRecord(ValueObject):
    """A record produced by an ingester container, parsed from records.jsonl.

    Replaces raw dicts with typed fields so downstream handlers
    don't need fragile `.get("source_id", .get("id", ""))` patterns.
    """

    source_id: str
    metadata: dict[str, Any]
    files: list[IngesterFileRef] = []

    @property
    def total_file_mb(self) -> float:
        """Sum of all file sizes in megabytes."""
        return sum(f.size_mb for f in self.files)

    @classmethod
    def from_jsonl(cls, path: Path) -> list["IngesterRecord"]:
        """Parse records.jsonl into typed IngesterRecord objects."""
        records: list[IngesterRecord] = []
        if not path.exists():
            return records
        for line in path.open():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                files_raw = data.get("files", [])
                files = [IngesterFileRef.model_validate(f) for f in files_raw]
                records.append(
                    IngesterRecord(
                        source_id=data.get("source_id", data.get("id", "")),
                        metadata=data.get("metadata", {}),
                        files=files,
                    )
                )
            except (json.JSONDecodeError, ValueError):
                logger.warning("Skipping malformed ingester record line")
        return records
