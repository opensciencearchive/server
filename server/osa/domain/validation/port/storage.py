"""Storage port scoped to the validation domain."""

from abc import abstractmethod
from pathlib import Path
from typing import Protocol

from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port
from osa.domain.validation.model.batch_outcome import BatchRecordOutcome, HookRecordId


class HookStoragePort(Port, Protocol):
    """File storage operations used by the validation domain."""

    @abstractmethod
    def get_hook_output_dir(self, deposition_srn: DepositionSRN, hook_name: str) -> Path:
        """Return the durable output directory for a hook's results."""
        ...

    @abstractmethod
    def get_files_dir(self, deposition_id: DepositionSRN) -> Path:
        """Return the directory containing data files for a deposition."""
        ...

    @abstractmethod
    async def write_checkpoint(
        self, work_dir: Path, outcomes: dict[HookRecordId, BatchRecordOutcome]
    ) -> None:
        """Atomically write checkpoint JSONL to work_dir/_checkpoint.jsonl."""
        ...

    @abstractmethod
    async def write_batch_outcomes(
        self,
        work_dir: Path,
        outcomes: dict[HookRecordId, BatchRecordOutcome],
    ) -> None:
        """Write canonical features.jsonl, rejections.jsonl, errors.jsonl."""
        ...

    @abstractmethod
    async def read_batch_outcomes(
        self, output_dir: str, hook_name: str
    ) -> dict[HookRecordId, BatchRecordOutcome]:
        """Read JSONL batch outputs (features/rejections/errors) for a hook."""
        ...
