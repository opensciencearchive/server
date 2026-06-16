"""Storage port scoped to the validation domain."""

from abc import abstractmethod
from collections.abc import AsyncIterator
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
    async def write_run_ref(self, work_dir: Path, run_id: str, release_id: str) -> None:
        """Write ``{work_dir}/output/run.json`` carrying this run's provenance.

        The feature-insert handler reads it back to stamp ``feature.run_id`` —
        no DB run-id lookup (design-revisions §6). #145.
        """
        ...

    @abstractmethod
    async def write_hook_log(self, work_dir: Path, text: str) -> str:
        """Write a failed hook container's logs to ``{work_dir}/output/hook.log``.

        Returns the locator stored as ``HookRun.log_ref`` (#145/#147). The logs are
        tenant-scoped — written beside the hook's other outputs, never to operator
        logs. Retrieval is served by the authenticated #147 endpoint.
        """
        ...

    @abstractmethod
    async def read_hook_log(self, log_ref: str) -> AsyncIterator[bytes]:
        """Stream a captured hook log back by its stored ``log_ref`` locator (#147).

        Reads the artifact written by :meth:`write_hook_log` (deposition or
        ingestion path — the locator is self-contained). Raises ``NotFoundError``
        if the locator is absent; rejects a locator that escapes the data root.
        """
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
