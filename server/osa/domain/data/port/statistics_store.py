"""Port for instance statistics and ``table_statistics`` verification (#219).

Lives in the data domain: statistics are read-surface content (dashboard,
manifest, SKILL), and the verifier is the read surface's defence in depth.
"""

from __future__ import annotations

from typing import Protocol

from osa.domain.data.model.statistics import InstanceStats, StatisticsDrift


class StatisticsStore(Protocol):
    """Instance snapshot + lockstep-count reads + the verifier's truth query.

    The snapshot holds what only polling the storage engine can observe
    (storage bytes). Row counts come from the lockstep-maintained
    ``table_statistics`` — never recounted on request paths. The verifier
    methods hold the ONLY sanctioned post-deploy whole-table counting.
    """

    async def count_this_month(self) -> int:
        """Records published since the start of the current month.

        Live but bounded: an index-served month window over
        ``idx_records_published_at``, never a full-table count.
        """
        ...

    async def records_total(self) -> int:
        """Total records across schemas — SUM over ``table_statistics``."""
        ...

    async def read_snapshot(self) -> InstanceStats | None:
        """The last materialized snapshot, or None if never refreshed."""
        ...

    async def compute_snapshot(self) -> InstanceStats:
        """Compute the snapshot: sampled storage bytes + summed lockstep counts."""
        ...

    async def refresh(self) -> None:
        """Recompute and upsert the singleton snapshot row."""
        ...

    async def table_statistics_drift(self) -> list[StatisticsDrift]:
        """Recompute true counts and diff them against ``table_statistics``.

        The truth query is the backfill migration's, kept runnable — this and
        repair are the only sanctioned whole-table counting after deploy.
        """
        ...

    async def repair_table_statistics(self) -> None:
        """Overwrite ``table_statistics`` with recomputed truth (admin repair)."""
        ...
