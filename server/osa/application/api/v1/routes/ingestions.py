"""Ingest REST routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.ingest.command.start_ingest import (
    IngestRunCreated,
    StartIngest,
    StartIngestHandler,
)

router = APIRouter(prefix="/ingestions", tags=["Ingestions"], route_class=DishkaRoute)


@router.post("", response_model=IngestRunCreated, status_code=201)
async def start_ingest(
    body: StartIngest,
    handler: FromDishka[StartIngestHandler],
) -> IngestRunCreated:
    return await handler.run(body)
