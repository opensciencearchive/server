"""Port for reading and refreshing the instance-statistics snapshot."""

from __future__ import annotations

from typing import Protocol

from osa.domain.record.model.statistics import InstanceStats


class StatisticsStore(Protocol):
    """Reads the materialized instance-statistics snapshot and refreshes it.

    The snapshot holds the expensive aggregates (storage, feature rows). Cheap
    counts (this-month) are exposed here as live queries so the read path stays
    fresh without a refresh cycle.
    """

    async def count_this_month(self) -> int:
        """Records published since the start of the current month (live)."""
        ...

    async def read_snapshot(self) -> InstanceStats | None:
        """The last materialized snapshot, or None if never refreshed."""
        ...

    async def compute_snapshot(self) -> InstanceStats:
        """Compute the aggregates live (cold-start fallback; O(rows))."""
        ...

    async def refresh(self) -> None:
        """Recompute and upsert the singleton snapshot row."""
        ...
