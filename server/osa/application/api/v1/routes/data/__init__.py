"""Unified ``/data/`` read surface router.

Subroutes are registered by the user-story phases:
- catalog + manifest (``GET /data``, ``GET /data/{schema}``) — US3
- single record by ID (``GET /data/records/{id}``) — US4
- records table matrix (``/data/{schema}/records*``) — US1/US2 via the factory
- feature table matrix (``/data/{schema}/{feature}*``) — US5 via the factory
"""

from __future__ import annotations

from dishka.integrations.fastapi import DishkaRoute
from fastapi import APIRouter

from osa.application.api.v1.routes.data import (
    catalog,
    features_table,
    records,
    records_table,
)
from osa.domain.data.model.catalog import NodeCatalog

router = APIRouter(prefix="/data", tags=["data"], route_class=DishkaRoute)

# ``GET /data`` (empty sub-path) is registered directly on the prefixed router.
router.add_api_route(
    "",
    catalog.get_node_catalog,
    methods=["GET"],
    operation_id="data_get_node_catalog",
    response_model=NodeCatalog,
)

# Table-shaped routes (records + feature tables) are registered via the factory
# onto a DishkaRoute-enabled subrouter. Records is registered before the feature
# table's ``/{schema}/{feature}`` catch-all so ``/{schema}/records`` matches the
# records routes rather than being captured as a feature named "records".
tables_router = APIRouter(route_class=DishkaRoute)
records_table.register(tables_router)
features_table.register(tables_router)

# Order matters: literal ``/records/{id}`` and the ``/{schema}/records*`` table
# routes must precede the manifest's ``/{schema}`` catch-all so a record fetch
# or table read isn't captured as a schema manifest lookup.
router.include_router(records.router)
router.include_router(tables_router)
router.include_router(catalog.manifest_router)
