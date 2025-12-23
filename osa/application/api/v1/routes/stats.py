"""Stats API routes."""

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from pydantic import BaseModel

from osa.domain.index.model.registry import IndexRegistry
from osa.domain.record.port.repository import RecordRepository

router = APIRouter(
    prefix="/stats",
    tags=["stats"],
    route_class=DishkaRoute,
)


class IndexStats(BaseModel):
    """Stats for a single index."""

    name: str
    count: int
    healthy: bool


class StatsResponse(BaseModel):
    """System statistics response."""

    records: int
    indexes: list[IndexStats]


@router.get("")
async def get_stats(
    record_repo: FromDishka[RecordRepository],
    indexes: FromDishka[IndexRegistry],
) -> StatsResponse:
    """Get system statistics."""
    record_count = await record_repo.count()

    index_stats = []
    for name, backend in indexes.items():
        try:
            count = await backend.count()
            healthy = await backend.health()
        except Exception:
            count = 0
            healthy = False

        index_stats.append(IndexStats(name=name, count=count, healthy=healthy))

    return StatsResponse(records=record_count, indexes=index_stats)
