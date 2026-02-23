"""SourceRunner port — interface for executing source containers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from osa.domain.shared.model.source import SourceDefinition


@dataclass(frozen=True)
class SourceInputs:
    """Inputs for a source container run."""

    config: dict[str, Any] | None = None
    since: datetime | None = None
    limit: int | None = None
    offset: int = 0
    session: dict[str, Any] | None = None


@dataclass(frozen=True)
class SourceOutput:
    """Output from a source container run."""

    records: list[dict[str, Any]]  # Parsed from records.jsonl
    session: dict[str, Any] | None  # From session.json (continuation)
    files_dir: Path  # Where source wrote data files


class SourceRunner(Protocol):
    """Protocol for executing source containers."""

    async def run(
        self,
        source: SourceDefinition,
        inputs: SourceInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> SourceOutput: ...
