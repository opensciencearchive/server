"""SearchRecords query — search and filter published records."""

from osa.domain.discovery.model.value import (
    Filter,
    RecordSearchResult,
    SortOrder,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.query import Query, QueryHandler, Result


class SearchRecords(Query):
    filters: list[Filter] = []
    q: str | None = None
    sort: str = "published_at"
    order: SortOrder = SortOrder.DESC
    cursor: str | None = None
    limit: int = 20


class SearchRecordsResult(Result):
    results: list[dict]
    total: int
    cursor: str | None
    has_more: bool


class SearchRecordsHandler(QueryHandler[SearchRecords, SearchRecordsResult]):
    __auth__ = public()
    discovery_service: DiscoveryService

    async def run(self, cmd: SearchRecords) -> SearchRecordsResult:
        result: RecordSearchResult = await self.discovery_service.search_records(
            filters=cmd.filters,
            q=cmd.q,
            sort=cmd.sort,
            order=cmd.order,
            cursor=cmd.cursor,
            limit=cmd.limit,
        )
        return SearchRecordsResult(
            results=[
                {
                    "srn": str(r.srn),
                    "published_at": r.published_at.isoformat(),
                    "metadata": r.metadata,
                }
                for r in result.results
            ],
            total=result.total,
            cursor=result.cursor,
            has_more=result.has_more,
        )
