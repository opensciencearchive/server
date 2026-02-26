"""Storage port scoped to the source domain."""

from abc import abstractmethod
from pathlib import Path
from typing import Protocol

from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN
from osa.domain.shared.port import Port


class SourceStoragePort(Port, Protocol):
    """File storage operations used by the source domain."""

    @abstractmethod
    def get_source_staging_dir(self, convention_srn: ConventionSRN, run_id: str) -> Path:
        """Staging dir for source-ingested files, isolated per run."""
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
        """Rename source staging files into the deposition's canonical file location."""
        ...
