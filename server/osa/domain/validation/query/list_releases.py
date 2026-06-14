"""ListReleases — a hook's release history (#145, US3/US4).

``GET /hooks/{name}/releases`` returns every release for a hook,
version-descending, alongside the currently-live version (FR-010).
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from osa.domain.shared.authorization.gate import public
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.query import Query, QueryHandler, Result
from osa.domain.validation.service.hook_registry import HookRegistryService


class ListReleases(Query):
    name: HookName


class ReleaseSummary(BaseModel):
    version: int
    digest: str
    image: str
    source_ref: str
    built_by: str | None
    built_at: datetime


class ReleaseList(Result):
    hook_name: HookName
    live_version: int | None
    releases: list[ReleaseSummary]


class ListReleasesHandler(QueryHandler[ListReleases, ReleaseList]):
    __auth__ = public()
    service: HookRegistryService

    async def run(self, cmd: ListReleases) -> ReleaseList:
        hook = await self.service.get_hook(cmd.name)
        if hook is None:
            raise NotFoundError(f"Hook not found: {cmd.name}")
        releases = await self.service.list_releases(cmd.name)
        live_version = next((r.version for r in releases if r.id == hook.live_release_id), None)
        return ReleaseList(
            hook_name=cmd.name,
            live_version=live_version,
            releases=[
                ReleaseSummary(
                    version=r.version,
                    digest=r.runtime.digest,
                    image=r.runtime.image,
                    source_ref=r.source_ref,
                    built_by=r.built_by,
                    built_at=r.built_at,
                )
                for r in releases
            ],
        )
