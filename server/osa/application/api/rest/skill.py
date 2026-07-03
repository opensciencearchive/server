"""Unversioned root routes — the agent bootstrap surface (#151).

Only ``GET /`` (root discovery JSON) and ``GET /SKILL.md`` live at the domain
root (research §2); the per-schema reference doc is a ``.md`` representation
on the versioned data surface. Thin routes: handler → JSON / markdown
Response — no business logic here.
"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Request, Response

from osa.domain.data.model.skill import RootDiscovery
from osa.domain.data.query.skill import (
    GetRootDiscovery,
    GetRootDiscoveryHandler,
    GetSkillDocument,
    GetSkillDocumentHandler,
)

MARKDOWN_MEDIA_TYPE = "text/markdown; charset=utf-8"

router = APIRouter(tags=["skill"], route_class=DishkaRoute)


@router.get("/", operation_id="skill_get_root_discovery", response_model=RootDiscovery)
async def get_root_discovery(
    request: Request, handler: FromDishka[GetRootDiscoveryHandler]
) -> RootDiscovery:
    """Root discovery document: node identity + pointers for agents."""
    return await handler.run(GetRootDiscovery(openapi_path=request.app.openapi_url or ""))


@router.get("/SKILL.md", operation_id="skill_get_skill_document")
async def get_skill_document(handler: FromDishka[GetSkillDocumentHandler]) -> Response:
    """Generated agent-skill index (markdown), rendered from the live catalog."""
    content = await handler.run(GetSkillDocument())
    return Response(content=content, media_type=MARKDOWN_MEDIA_TYPE)
