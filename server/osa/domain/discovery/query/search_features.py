"""SearchFeatures query — query and filter rows in a specific feature table."""

from osa.domain.discovery.model.value import (
    FeatureSearchResult,
    Filter,
    SortOrder,
)
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import RecordSRN
from osa.domain.shared.query import Query, QueryHandler, Result


class SearchFeatures(Query):
    hook_name: str
    filters: list[Filter] = []
    record_srn: str | None = None
    sort: str = "id"
    order: SortOrder = SortOrder.DESC
    cursor: str | None = None
    limit: int = 50


class SearchFeaturesResult(Result):
    rows: list[dict]
    cursor: str | None
    has_more: bool


class SearchFeaturesHandler(QueryHandler[SearchFeatures, SearchFeaturesResult]):
    __auth__ = public()
    discovery_service: DiscoveryService

    async def run(self, cmd: SearchFeatures) -> SearchFeaturesResult:
        record_srn: RecordSRN | None = None
        if cmd.record_srn:
            try:
                record_srn = RecordSRN.parse(cmd.record_srn)
            except ValueError as exc:
                raise ValidationError(str(exc), field="record_srn") from exc
        result: FeatureSearchResult = await self.discovery_service.search_features(
            hook_name=cmd.hook_name,
            filters=cmd.filters,
            record_srn=record_srn,
            sort=cmd.sort,
            order=cmd.order,
            cursor=cmd.cursor,
            limit=cmd.limit,
        )
        return SearchFeaturesResult(
            rows=[{"record_srn": str(r.record_srn), **r.data} for r in result.rows],
            cursor=result.cursor,
            has_more=result.has_more,
        )
