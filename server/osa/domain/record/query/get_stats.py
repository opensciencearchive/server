"""GetStats query handler — public node statistics (record count)."""

from osa.domain.record.service.record import RecordService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.query import Query, QueryHandler, Result


class GetStats(Query):
    pass


class StatsResult(Result):
    records: int


class GetStatsHandler(QueryHandler[GetStats, StatsResult]):
    __auth__ = public()
    record_service: RecordService

    async def run(self, cmd: GetStats) -> StatsResult:
        return StatsResult(records=await self.record_service.count())
