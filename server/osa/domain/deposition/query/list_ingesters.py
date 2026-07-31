"""ListIngesters — the ingester catalog.

There is no standalone ingester registry: an ingester is the
``IngesterDefinition`` embedded on a convention (``Convention.ingester``). This
synthesizes a catalog by scanning conventions that have one configured, mirroring
the shape of the hook catalog (``GET /hooks``).
"""

from __future__ import annotations

from pydantic import BaseModel

from osa.domain.auth.model.principal import Principal
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.model.srn import ConventionSlug
from osa.domain.shared.query import Query, QueryHandler, Result


class ListIngesters(Query):
    pass


class IngesterCatalogItem(BaseModel):
    name: ConventionSlug  # the owning convention's slug
    title: str
    description: str
    schema_id: str  # rendered "<id>@<semver>"
    image: str
    digest: str
    source_ref: str | None
    schedule: str | None  # cron expression, if the ingester runs periodically


class IngesterCatalog(Result):
    items: list[IngesterCatalogItem]


class ListIngestersHandler(QueryHandler[ListIngesters, IngesterCatalog]):
    # Scoped M2M read (#184): the ingester catalog exposes image/digest/source
    # provenance, so it sits behind `ingesters:read` (OR ADMIN) rather than being
    # public. The self-host BFF proxies it with a SUPERADMIN token (ADMIN → allowed).
    __auth__ = requires_scope("ingesters:read")

    principal: Principal
    convention_service: ConventionService

    async def run(self, cmd: ListIngesters) -> IngesterCatalog:
        conventions = await self.convention_service.list_conventions_with_source()
        items: list[IngesterCatalogItem] = []
        for c in conventions:
            ingester = c.ingester
            if ingester is None:
                continue
            items.append(
                IngesterCatalogItem(
                    name=c.id,
                    title=c.title,
                    description=c.description,
                    schema_id=c.schema_id.render(),
                    image=ingester.image,
                    digest=ingester.digest,
                    source_ref=ingester.source_ref,
                    schedule=ingester.schedule.cron if ingester.schedule else None,
                )
            )
        return IngesterCatalog(items=items)
