"""Port for executing hooks in OCI containers."""

from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.port import Port
from osa.domain.validation.model.hook_result import HookResult


@dataclass(frozen=True)
class HookInputs:
    """Inputs to pass to a hook container."""

    record_json: dict
    files_dir: Path | None = None
    config: dict | None = None


@runtime_checkable
class HookRunner(Port, Protocol):
    """Execute hooks in OCI containers."""

    @abstractmethod
    async def run(
        self,
        hook: HookDefinition,
        inputs: HookInputs,
        workspace_dir: Path,
    ) -> HookResult:
        """Run a hook and return its result."""
        ...
