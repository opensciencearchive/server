"""Port for managing feature tables and inserting hook-derived features."""

from abc import abstractmethod
from typing import Any, Protocol, runtime_checkable

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.port import Port


@runtime_checkable
class FeatureStore(Port, Protocol):
    """Manages feature tables for hook-derived data."""

    @abstractmethod
    async def create_table(self, hook_name: str, columns: list[ColumnDef]) -> None:
        """Create a feature table for a hook. Fails on name collision."""
        ...

    @abstractmethod
    async def insert_features(
        self,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert feature rows into the feature table. Returns row count."""
        ...
