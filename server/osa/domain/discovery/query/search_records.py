"""SearchRecords query — search and filter published records."""

from typing import Any

from osa.domain.discovery.model.value import (
    FilterExpr,
    RecordSearchResult,
    SortOrder,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class SearchRecords(Query):
    filter_expr: FilterExpr | None = None
    schema_srn: SchemaSRN | None = None
    convention_srn: ConventionSRN | None = None
    q: str | None = None
    sort: str = "published_at"
    order: SortOrder = SortOrder.DESC
    cursor: str | None = None
    limit: int = 20


class SearchRecordsResult(Result):
    results: list[dict[str, Any]]
    cursor: str | None
    has_more: bool


class SearchRecordsHandler(QueryHandler[SearchRecords, SearchRecordsResult]):
    __auth__ = public()
    discovery_service: DiscoveryService

    async def run(self, cmd: SearchRecords) -> SearchRecordsResult:
        result: RecordSearchResult = await self.discovery_service.search_records(
            filter_expr=cmd.filter_expr,
            schema_srn=cmd.schema_srn,
            convention_srn=cmd.convention_srn,
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
            cursor=result.cursor,
            has_more=result.has_more,
        )
