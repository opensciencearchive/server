"""Storage port scoped to the feature domain."""

from abc import abstractmethod
from typing import Any, Protocol

from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class FeatureStoragePort(Port, Protocol):
    """File storage operations used by the feature domain."""

    @abstractmethod
    async def read_hook_features(
        self, deposition_srn: DepositionSRN, hook_name: str
    ) -> list[dict[str, Any]]:
        """Read features.json from a hook's output directory."""
        ...

    @abstractmethod
    async def hook_features_exist(self, deposition_srn: DepositionSRN, hook_name: str) -> bool:
        """Check whether features.json exists in a hook's output directory."""
        ...
