"""Port for managing feature tables and inserting hook-derived features."""

from abc import abstractmethod
from typing import Any, Protocol, runtime_checkable

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.port import Port


@runtime_checkable
class FeatureStore(Port, Protocol):
    """Manages feature tables for hook-derived data."""

    @abstractmethod
    async def create_tables(self, convention_id: str, hooks: list[HookDefinition]) -> None:
        """Create feature tables for each hook in a convention."""
        ...

    @abstractmethod
    async def insert_features(
        self,
        convention_id: str,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert feature rows into the feature table. Returns row count."""
        ...
