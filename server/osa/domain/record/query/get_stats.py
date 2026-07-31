"""GetStats query handler — public node statistics."""

from datetime import datetime

from osa.domain.record.port.statistics_store import StatisticsStore
from osa.domain.record.service.record import RecordService
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
    """Node statistics: live counts + the materialized storage/feature snapshot.

    ``records`` and ``records_this_month`` are read live (cheap, indexed) so they
    stay fresh; ``storage_bytes`` and ``features_per_record`` come from the
    periodically-refreshed snapshot, falling back to a live computation on cold
    start (before the first refresh).
    """

    __auth__ = public()
    record_service: RecordService
    stats_store: StatisticsStore

    async def run(self, cmd: GetStats) -> StatsResult:
        records = await self.record_service.count()
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
