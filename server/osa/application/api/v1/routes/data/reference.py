"""Schema reference route — ``GET /data/{schema}.md`` (#151).

The markdown representation of the schema resource, following the data
surface's suffix convention (``.csv``/``.csv.gz`` precedent). Registered
before the ``/{schema}`` manifest catch-all (longest-suffix-first);
resolution reuses ``resolve_schema`` (unknown/reserved ids → 404).
"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, Response

from osa.domain.data.query.skill import GetSchemaReference, GetSchemaReferenceHandler

MARKDOWN_MEDIA_TYPE = "text/markdown; charset=utf-8"

router = APIRouter(route_class=DishkaRoute)


@router.get("/{schema}.md", operation_id="data_get_schema_reference")
async def get_schema_reference(
    schema: str, handler: FromDishka[GetSchemaReferenceHandler]
) -> Response:
    """Reference doc for a schema (`<id>` or `<id>@<semver>`), as markdown."""
    content = await handler.run(GetSchemaReference(schema=schema))
    return Response(content=content, media_type=MARKDOWN_MEDIA_TYPE)
