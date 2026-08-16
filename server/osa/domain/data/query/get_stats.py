"""GetStats query handler — public node statistics (data read surface).

Relocated from the record domain with #219 phase 6: statistics are read-surface
content, and the records total now comes from the lockstep-maintained
``table_statistics`` (SUM over stored counts) instead of a full-table COUNT on
the request path. ``records_this_month`` stays live — an index-served month
window, the one sanctioned counting statement here.
"""

from datetime import datetime

from osa.domain.data.port.statistics_store import StatisticsStore
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.query import Query, QueryHandler, Result


class GetStats(Query):
    pass


class StatsResult(Result):
    records: int
    records_this_month: int
    storage_bytes: int
    features_per_record: float
    computed_at: datetime | None


class GetStatsHandler(QueryHandler[GetStats, StatsResult]):
    """Node statistics: lockstep counts + the materialized storage snapshot."""

    __auth__ = public()
    stats_store: StatisticsStore

    async def run(self, cmd: GetStats) -> StatsResult:
        records = await self.stats_store.records_total()
        records_this_month = await self.stats_store.count_this_month()

        snapshot = await self.stats_store.read_snapshot()
        if snapshot is None:
            snapshot = await self.stats_store.compute_snapshot()

        features_per_record = snapshot.feature_rows / records if records else 0.0

        return StatsResult(
            records=records,
            records_this_month=records_this_month,
            storage_bytes=snapshot.storage_bytes,
            features_per_record=features_per_record,
            computed_at=snapshot.computed_at,
        )
