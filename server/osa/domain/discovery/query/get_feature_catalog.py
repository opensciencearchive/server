"""GetFeatureCatalog query — list available feature tables with schemas and counts."""

from osa.domain.discovery.model.value import FeatureCatalog
from osa.domain.discovery.service.discovery import DiscoveryService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.query import Query, QueryHandler, Result


class GetFeatureCatalog(Query):
    pass


class GetFeatureCatalogResult(Result):
    tables: list[dict]


class GetFeatureCatalogHandler(QueryHandler[GetFeatureCatalog, GetFeatureCatalogResult]):
    __auth__ = public()
    discovery_service: DiscoveryService

    async def run(self, cmd: GetFeatureCatalog) -> GetFeatureCatalogResult:
        catalog: FeatureCatalog = await self.discovery_service.get_feature_catalog()
        return GetFeatureCatalogResult(
            tables=[
                {
                    "hook_name": entry.hook_name,
                    "columns": [c.model_dump() for c in entry.columns],
                    "record_count": entry.record_count,
                }
                for entry in catalog.tables
            ]
        )
