"""Ingester catalog API routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.deposition.query.list_ingesters import (
    IngesterCatalog,
    ListIngesters,
    ListIngestersHandler,
)

router = APIRouter(
    prefix="/ingesters",
    tags=["ingesters"],
    route_class=DishkaRoute,
)


@router.get("")
async def list_ingesters(
    handler: FromDishka[ListIngestersHandler],
) -> IngesterCatalog:
    """List the node's configured ingesters (one per convention with a source)."""
    return await handler.run(ListIngesters())
