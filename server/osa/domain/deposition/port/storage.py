from abc import abstractmethod
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Protocol

from osa.domain.deposition.model.value import DepositionFile
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class FileStoragePort(Port, Protocol):
    """Storage operations scoped to the deposition domain.

    Hook output, hook features, and source staging methods have been
    moved to their respective domain ports (HookStoragePort,
    FeatureStoragePort, SourceStoragePort).
    """

    @abstractmethod
    def get_files_dir(self, deposition_id: DepositionSRN) -> Path:
        """Return the local directory containing uploaded files for a deposition."""
        ...

    @abstractmethod
    async def save_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
        content: bytes,
        size: int,
    ) -> DepositionFile: ...

    @abstractmethod
    async def get_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> AsyncIterator[bytes]: ...

    @abstractmethod
    async def delete_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> None: ...

    @abstractmethod
    async def delete_files_for_deposition(
        self,
        deposition_id: DepositionSRN,
    ) -> None: ...

    @abstractmethod
    def move_source_files_to_deposition(
        self,
        staging_dir: Path,
        source_id: str,
        deposition_srn: DepositionSRN,
    ) -> None:
        """Rename source staging files into the deposition's canonical file location.
        O(1) on local FS, server-side copy on S3."""
        ...
