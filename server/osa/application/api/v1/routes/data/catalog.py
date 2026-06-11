"""Catalog & manifest routes — ``GET /data`` and ``GET /data/{schema}``.

JSON-only (no format suffix). Reserved schema names (``records``, ``datasets``)
and unknown schemas surface as 404 via the handler's ``NotFoundError``.

The node-catalog handler is registered directly on the prefixed ``/data``
router (an empty sub-path can't be ``include_router``-ed); the manifest handler
lives on ``manifest_router`` so its ``/{schema}`` catch-all can be ordered
after the literal ``/records/{id}`` route.
"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.data.model.catalog import NodeCatalog
from osa.domain.data.model.manifest import SchemaManifest
from osa.domain.data.query.catalog import (
    GetNodeCatalog,
    GetNodeCatalogHandler,
    GetSchemaManifest,
    GetSchemaManifestHandler,
)

manifest_router = APIRouter(route_class=DishkaRoute)


async def get_node_catalog(handler: FromDishka[GetNodeCatalogHandler]) -> NodeCatalog:
    """List schemas hosted at this node."""
    return await handler.run(GetNodeCatalog())


@manifest_router.get(
    "/{schema}", operation_id="data_get_schema_manifest", response_model=SchemaManifest
)
async def get_schema_manifest(
    schema: str, handler: FromDishka[GetSchemaManifestHandler]
) -> SchemaManifest:
    """Machine-readable manifest for a schema (`<id>` or `<id>@<semver>`)."""
    return await handler.run(GetSchemaManifest(schema=schema))
