"""Storage port scoped to the validation domain."""

from abc import abstractmethod
from pathlib import Path
from typing import Protocol

from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class HookStoragePort(Port, Protocol):
    """File storage operations used by the validation domain."""

    @abstractmethod
    def get_hook_output_dir(self, deposition_srn: DepositionSRN, hook_name: str) -> Path:
        """Return the durable output directory for a hook's results."""
        ...
