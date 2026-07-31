"""Stats API routes."""

from datetime import datetime

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from pydantic import BaseModel

from osa.domain.record.query.get_stats import GetStats, GetStatsHandler

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

    ``records`` and ``records_this_month`` are live; ``storage_bytes`` and
    ``features_per_record`` come from the periodically-refreshed snapshot
    (``computed_at`` marks its freshness, null before the first refresh).
    """

    records: int
    records_this_month: int
    storage_bytes: int
    features_per_record: float
    computed_at: datetime | None
    data_url: str = "/api/v1/data"


@router.get("")
async def get_stats(
    handler: FromDishka[GetStatsHandler],
) -> StatsResponse:
    """Get system statistics."""
    result = await handler.run(GetStats())
    return StatsResponse(
        records=result.records,
        records_this_month=result.records_this_month,
        storage_bytes=result.storage_bytes,
        features_per_record=result.features_per_record,
        computed_at=result.computed_at,
    )
