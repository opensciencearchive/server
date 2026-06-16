"""ListHooks — the hook catalog (#145, US3).

``GET /hooks`` lists every hook with its fixed output contract and its current
live release (FR-011). A hook with no live release (none ever minted) reports
``live_release: null``.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.hook import HookName, TableFeatureSpec
from osa.domain.shared.query import Query, QueryHandler, Result
from osa.domain.validation.service.hook_registry import HookRegistryService


class ListHooks(Query):
    pass


class LiveReleaseSummary(BaseModel):
    version: int
    digest: str
    source_ref: str
    built_at: datetime


class HookCatalogItem(BaseModel):
    name: HookName
    feature: TableFeatureSpec
    live_release: LiveReleaseSummary | None


class HookCatalog(Result):
    items: list[HookCatalogItem]


class ListHooksHandler(QueryHandler[ListHooks, HookCatalog]):
    __auth__ = public()
    service: HookRegistryService

    async def run(self, cmd: ListHooks) -> HookCatalog:
        hooks = await self.service.list_hooks()
        live = await self.service.resolve_live([h.name for h in hooks])
        return HookCatalog(
            items=[
                HookCatalogItem(
                    name=h.name,
                    feature=h.feature,
                    live_release=(
                        LiveReleaseSummary(
                            version=rel.version,
                            digest=rel.runtime.digest,
                            source_ref=rel.source_ref,
                            built_at=rel.built_at,
                        )
                        if (rel := live.get(h.name)) is not None
                        else None
                    ),
                )
                for h in hooks
            ]
        )
