from abc import abstractmethod
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Protocol

from osa.domain.deposition.model.value import DepositionFile
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.shared.port import Port


class FileStoragePort(Port, Protocol):
    @abstractmethod
    def get_files_dir(self, deposition_id: DepositionSRN) -> Path:
        """Return the local directory containing uploaded files for a deposition."""
        ...

    @abstractmethod
    def get_hook_output_dir(self, deposition_id: DepositionSRN, hook_name: str) -> Path:
        """Return the durable output directory for a hook's results."""
        ...

    @abstractmethod
    async def read_hook_features(
        self, deposition_id: DepositionSRN, hook_name: str
    ) -> list[dict[str, Any]]:
        """Read features.json from a hook's output directory."""
        ...

    @abstractmethod
    async def hook_features_exist(self, deposition_id: DepositionSRN, hook_name: str) -> bool:
        """Check whether features.json exists in a hook's output directory."""
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
    def get_source_staging_dir(self, convention_srn: ConventionSRN) -> Path:
        """Staging dir for source-ingested files. Bind-mounted into source containers.
        Files written here are renamed into deposition dirs after deposition creation."""
        ...

    @abstractmethod
    def get_source_output_dir(self, convention_srn: ConventionSRN, run_id: str) -> Path:
        """Output dir for a source run (records.jsonl, session.json)."""
        ...

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
