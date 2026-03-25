"""Storage port scoped to the feature domain."""

from abc import abstractmethod
from typing import Any, Protocol

from osa.domain.shared.port import Port
from osa.domain.validation.model.batch_outcome import BatchRecordOutcome


class FeatureStoragePort(Port, Protocol):
    """File storage operations used by the feature domain."""

    @abstractmethod
    def get_hook_output_root(self, source_type: str, source_id: str) -> str:
        """Resolve the root directory containing hook outputs for a source.

        The handler uses this to locate hook outputs, then passes the
        resolved path to read_hook_features / hook_features_exist.
        """
        ...

    @abstractmethod
    async def read_hook_features(
        self, hook_output_dir: str, feature_name: str
    ) -> list[dict[str, Any]]:
        """Read features.json from a hook's output directory."""
        ...

    @abstractmethod
    async def hook_features_exist(self, hook_output_dir: str, feature_name: str) -> bool:
        """Check whether features.json exists in a hook's output directory."""
        ...

    @abstractmethod
    async def read_batch_outcomes(
        self, output_dir: str, hook_name: str
    ) -> dict[str, BatchRecordOutcome]:
        """Read JSONL batch outputs (features/rejections/errors) for a hook.

        Parses features.jsonl, rejections.jsonl, and errors.jsonl from the
        hook's output directory. Each record appears in exactly one file.

        Returns a dict keyed by record ID.
        """
        ...
