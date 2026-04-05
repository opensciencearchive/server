"""IngesterRunner port — interface for executing ingester containers.

Relocated from domain/source/ to shared/port/ since both the ingest
domain and infrastructure runners depend on this contract.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol

from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSRN


@dataclass(frozen=True)
class IngesterInputs:
    """Inputs for an ingester container run."""

    convention_srn: ConventionSRN
    ingest_run_id: str = ""
    batch_index: int = 0
    config: dict[str, Any] | None = None
    since: datetime | None = None
    limit: int | None = None
    offset: int = 0
    session: dict[str, Any] | None = None


@dataclass(frozen=True)
class IngesterOutput:
    """Output from an ingester container run."""

    records: list[dict[str, Any]]  # Parsed from records.jsonl
    session: dict[str, Any] | None  # From session.json (continuation)
    files_dir: Path  # Where ingester wrote data files


class IngesterRunner(Protocol):
    """Protocol for executing ingester containers."""

    async def run(
        self,
        ingester: IngesterDefinition,
        inputs: IngesterInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> IngesterOutput: ...

    async def capture_logs(self, run_id: str) -> str:
        """Capture recent container logs for a run.

        Returns the last few lines of container/pod output, or empty string
        if logs are unavailable. Used for failure diagnostics.
        """
        ...
