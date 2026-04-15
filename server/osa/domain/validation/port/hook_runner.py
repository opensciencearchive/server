"""Port for executing hooks in OCI containers."""

from abc import abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol, runtime_checkable

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.port import Port
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_result import HookResult


@dataclass(frozen=True)
class HookInputs:
    """Inputs to pass to a hook container.

    Uses the unified batch contract: records is a list of HookRecord
    (1 for depositions, N for ingests).
    files_dirs maps record ID → directory containing that record's files.
    """

    records: list[HookRecord]
    run_id: str
    files_dirs: dict[str, Path] = field(default_factory=dict)
    config: dict | None = None


@runtime_checkable
class HookRunner(Port, Protocol):
    """Execute hooks in OCI containers."""

    @abstractmethod
    async def run(
        self,
        hook: HookDefinition,
        inputs: HookInputs,
        work_dir: Path,
    ) -> HookResult:
        """Run a hook and return its result.

        The runner creates ``input/`` and ``output/`` subdirectories under *work_dir*.
        ``input/`` is ephemeral (cleaned after run); ``output/`` persists for later reading.
        """
        ...

    @abstractmethod
    async def capture_logs(self, run_id: str) -> str:
        """Capture recent container logs for a run.

        Returns the last few lines of container/pod output, or empty string
        if logs are unavailable. Used for failure diagnostics.
        """
        ...
