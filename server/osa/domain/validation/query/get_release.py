"""GetRelease — inspect a single hook release (#145, US3).

``GET /hooks/{name}/releases/{version}`` returns the full release, including
runtime config/limits and whether it is the hook's current live release.
"""

from __future__ import annotations

from datetime import datetime

from osa.domain.shared.authorization.gate import public
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import HookName, OciLimits
from osa.domain.shared.query import Query, QueryHandler, Result
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.service.hook_registry import HookRegistryService


class GetRelease(Query):
    name: HookName
    version: int


class ReleaseDetail(Result):
    hook_name: HookName
    version: int
    id: HookReleaseId
    image: str
    digest: str
    config: dict
    limits: OciLimits
    source_ref: str
    built_by: str | None
    built_at: datetime
    live: bool


class GetReleaseHandler(QueryHandler[GetRelease, ReleaseDetail]):
    __auth__ = public()
    service: HookRegistryService

    async def run(self, cmd: GetRelease) -> ReleaseDetail:
        release = await self.service.get_release(cmd.name, cmd.version)
        if release is None:
            raise NotFoundError(f"Release not found: {cmd.name}@v{cmd.version}")
        hook = await self.service.get_hook(cmd.name)
        is_live = hook is not None and hook.live_release_id == release.id
        return ReleaseDetail(
            hook_name=release.hook_name,
            version=release.version,
            id=release.id,
            image=release.runtime.image,
            digest=release.runtime.digest,
            config=release.runtime.config,
            limits=release.runtime.limits,
            source_ref=release.source_ref,
            built_by=release.built_by,
            built_at=release.built_at,
            live=is_live,
        )
