"""Port for managing feature tables and inserting hook-derived features."""

from abc import abstractmethod
from typing import Any, Protocol, runtime_checkable

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.port import Port


@runtime_checkable
class FeatureStore(Port, Protocol):
    """Manages feature tables for hook-derived data."""

    @abstractmethod
    async def create_table(self, feature: str, columns: list[ColumnDef]) -> None:
        """Create a feature table (named by its producing hook). Fails on collision."""
        ...

    @abstractmethod
    async def insert_features(
        self,
        feature: str,
        record_srn: str,
        rows: list[dict[str, Any]],
        run_id: str,
    ) -> int:
        """Insert feature rows into the feature table. Returns row count.

        ``run_id`` is the ``hook_runs.id`` that produced these rows; it is
        stamped on every row for per-row provenance (feature #145).
        """
        ...
