"""Storage port for the ingest domain.

Abstracts filesystem and S3 storage behind a single interface.
Both adapters delegate path computation to StorageLayout.
"""

from abc import abstractmethod
from pathlib import Path
from typing import Any, Protocol

from osa.domain.shared.port import Port


class IngestStoragePort(Port, Protocol):
    """Storage operations used by ingest domain handlers.

    Path-returning methods are locators — on filesystem they point to real
    directories (created by the adapter); on S3 they are PVC subpath strings
    used by K8s runners for volume mount computation (no I/O).
    """

    @abstractmethod
    async def read_session(self, ingest_run_srn: str) -> dict[str, Any] | None:
        """Read session state for ingester continuation. Returns None if no session."""
        ...

    @abstractmethod
    async def write_session(self, ingest_run_srn: str, session: dict[str, Any]) -> None:
        """Persist session state between batches."""
        ...

    @abstractmethod
    async def write_records(
        self, ingest_run_srn: str, batch_index: int, records: list[dict[str, Any]]
    ) -> None:
        """Write ingester output records for a batch as JSONL."""
        ...

    @abstractmethod
    async def read_records(self, ingest_run_srn: str, batch_index: int) -> list[dict[str, Any]]:
        """Read raw ingester output records for a batch."""
        ...

    @abstractmethod
    def batch_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        """Return the batch-level directory (parent of ingester/ and hooks/)."""
        ...

    @abstractmethod
    def batch_work_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        """Return the ingester work directory for a batch."""
        ...

    @abstractmethod
    def batch_files_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        """Return the files directory for a batch."""
        ...

    @abstractmethod
    def hook_work_dir(self, ingest_run_srn: str, batch_index: int, hook_name: str) -> Path:
        """Return the hook output directory for a batch."""
        ...
