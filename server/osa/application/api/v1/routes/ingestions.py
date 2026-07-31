"""Ingest REST routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter

from osa.domain.ingest.command.start_ingest import (
    IngestRunCreated,
    StartIngest,
    StartIngestHandler,
)
from osa.domain.ingest.model.ingest_run import IngestRunId
from osa.domain.ingest.query.get_ingestion import (
    GetIngestion,
    GetIngestionHandler,
    IngestRunDetail,
)
from osa.domain.ingest.query.list_ingestions import (
    IngestRunList,
    ListIngestions,
    ListIngestionsHandler,
)

router = APIRouter(prefix="/ingestions", tags=["Ingestions"], route_class=DishkaRoute)


@router.post("", response_model=IngestRunCreated, status_code=201)
async def start_ingest(
    body: StartIngest,
    handler: FromDishka[StartIngestHandler],
) -> IngestRunCreated:
    return await handler.run(body)


@router.get("", response_model=IngestRunList)
async def list_ingestions(
    handler: FromDishka[ListIngestionsHandler],
) -> IngestRunList:
    """List recent ingest runs, including pending/running ones. ADMIN only."""
    return await handler.run(ListIngestions())


@router.get("/{ingest_run_id}", response_model=IngestRunDetail)
async def get_ingestion(
    ingest_run_id: str,
    handler: FromDishka[GetIngestionHandler],
) -> IngestRunDetail:
    return await handler.run(GetIngestion(ingest_run_id=IngestRunId(ingest_run_id)))
