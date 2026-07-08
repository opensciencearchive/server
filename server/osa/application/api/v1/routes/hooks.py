"""Hook registry REST routes (#145) — releases, live pointer, catalog.

Thin HTTP ↔ DTO coercion only. The path ``{name}`` is parsed into a
:class:`HookName`; release/live bodies are edge DTOs carrying just the
payload (the hook is identified by the path). All business logic lives in
``HookRegistryService`` behind the command/query handlers; domain errors map
centrally in ``application/api/v1/errors.py``.
"""

from typing import Any
from uuid import UUID

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

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
from osa.domain.validation.model.hook_run import HookRunId
from osa.domain.validation.query.get_hook_run import (
    GetHookRun,
    GetHookRunHandler,
    HookRunDetail,
)
from osa.domain.validation.query.get_hook_run_logs import (
    GetHookRunLogs,
    GetHookRunLogsHandler,
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
    """Release payload — byte-identical to the deploy's ``release`` block.

    Strict (``extra="forbid"`` + required ``config``) so a misshaped client
    payload 422s here rather than silently running with an empty config.
    """

    model_config = ConfigDict(extra="forbid")

    image: str
    digest: str
    # Opaque, image-defined JSON object forwarded verbatim to the container.
    # Required (don't default a dropped config to {}).
    config: dict[str, Any]
    limits: OciLimits = Field(default_factory=OciLimits)
    source_ref: str


class SetLiveBody(BaseModel):
    model_config = ConfigDict(extra="forbid")

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


@router.get("/runs/{run_id}", response_model=HookRunDetail)
async def get_hook_run(
    run_id: UUID,
    handler: FromDishka[GetHookRunHandler],
) -> HookRunDetail:
    return await handler.run(GetHookRun(run_id=HookRunId(run_id)))


@router.get("/runs/{run_id}/logs")
async def get_hook_run_logs(
    run_id: UUID,
    handler: FromDishka[GetHookRunLogsHandler],
) -> StreamingResponse:
    result = await handler.run(GetHookRunLogs(run_id=HookRunId(run_id)))
    return StreamingResponse(result.stream, media_type="text/plain")
