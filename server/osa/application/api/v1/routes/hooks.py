"""Hook registry REST routes (#145) — releases, live pointer, catalog.

Thin HTTP ↔ DTO coercion only. The path ``{name}`` is parsed into a
:class:`HookName`; release/live bodies are edge DTOs carrying just the
payload (the hook is identified by the path). All business logic lives in
``HookRegistryService`` behind the command/query handlers; domain errors map
centrally in ``application/api/v1/errors.py``.
"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Response
from pydantic import BaseModel, Field

from osa.domain.shared.model.hook import HookName, OciLimits
from osa.domain.validation.command.create_release import (
    CreateRelease,
    CreateReleaseHandler,
    ReleaseCreated,
)
from osa.domain.validation.command.set_live import (
    LiveSet,
    SetLive,
    SetLiveHandler,
)
from osa.domain.validation.query.get_release import (
    GetRelease,
    GetReleaseHandler,
    ReleaseDetail,
)
from osa.domain.validation.query.list_hooks import (
    HookCatalog,
    ListHooks,
    ListHooksHandler,
)
from osa.domain.validation.query.list_releases import (
    ListReleases,
    ListReleasesHandler,
    ReleaseList,
)

router = APIRouter(prefix="/hooks", tags=["Hooks"], route_class=DishkaRoute)


class CreateReleaseBody(BaseModel):
    """Release payload — byte-identical to the deploy's ``release`` block."""

    image: str
    digest: str
    config: dict = Field(default_factory=dict)
    limits: OciLimits = Field(default_factory=OciLimits)
    source_ref: str


class SetLiveBody(BaseModel):
    version: int


@router.post("/{name}/releases", response_model=ReleaseCreated)
async def create_release(
    name: str,
    body: CreateReleaseBody,
    handler: FromDishka[CreateReleaseHandler],
    response: Response,
) -> ReleaseCreated:
    result = await handler.run(
        CreateRelease(
            name=HookName(name),
            image=body.image,
            digest=body.digest,
            config=body.config,
            limits=body.limits,
            source_ref=body.source_ref,
        )
    )
    response.status_code = 201 if result.created else 200
    return result


@router.put("/{name}/live", response_model=LiveSet)
async def set_live(
    name: str,
    body: SetLiveBody,
    handler: FromDishka[SetLiveHandler],
) -> LiveSet:
    return await handler.run(SetLive(name=HookName(name), version=body.version))


@router.get("", response_model=HookCatalog)
async def list_hooks(
    handler: FromDishka[ListHooksHandler],
) -> HookCatalog:
    return await handler.run(ListHooks())


@router.get("/{name}/releases", response_model=ReleaseList)
async def list_releases(
    name: str,
    handler: FromDishka[ListReleasesHandler],
) -> ReleaseList:
    return await handler.run(ListReleases(name=HookName(name)))


@router.get("/{name}/releases/{version}", response_model=ReleaseDetail)
async def get_release(
    name: str,
    version: int,
    handler: FromDishka[GetReleaseHandler],
) -> ReleaseDetail:
    return await handler.run(GetRelease(name=HookName(name), version=version))
