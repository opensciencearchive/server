"""Stats API routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from pydantic import BaseModel

from osa.domain.record.port.repository import RecordRepository

router = APIRouter(
    prefix="/stats",
    tags=["stats"],
    route_class=DishkaRoute,
)


class StatsResponse(BaseModel):
    """System statistics response.

    The legacy ``indexes`` field was removed with the index domain (the unified
    ``/data/`` surface replaces vector/keyword index reads). Per-schema and
    per-feature-table row counts now live in each schema manifest at
    ``GET /api/v1/data/{schema}``; ``data_url`` points there.
    """

    records: int
    data_url: str = "/api/v1/data"


@router.get("")
async def get_stats(
    record_repo: FromDishka[RecordRepository],
) -> StatsResponse:
    """Get system statistics."""
    record_count = await record_repo.count()
    return StatsResponse(records=record_count)
