"""Discovery API routes — search and filter records and features."""

from typing import Any

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter
from pydantic import BaseModel, Field

from osa.domain.discovery.model.value import (
    Filter,
    SortOrder,
)
from osa.domain.discovery.query.get_feature_catalog import (
    GetFeatureCatalog,
    GetFeatureCatalogHandler,
    GetFeatureCatalogResult,
)
from osa.domain.discovery.query.search_features import (
    SearchFeatures,
    SearchFeaturesHandler,
    SearchFeaturesResult,
)
from osa.domain.discovery.query.search_records import (
    SearchRecords,
    SearchRecordsHandler,
    SearchRecordsResult,
)

router = APIRouter(
    prefix="/discovery",
    tags=["discovery"],
    route_class=DishkaRoute,
)


# ── Request / Response models ──


class RecordSearchRequest(BaseModel):
    filters: list[Filter] = []
    q: str | None = None
    sort: str = "published_at"
    order: SortOrder = SortOrder.DESC
    cursor: str | None = None
    limit: int = Field(default=20, ge=1, le=100)


class RecordSearchResponse(BaseModel):
    results: list[dict[str, Any]]
    total: int
    cursor: str | None
    has_more: bool


class FeatureCatalogResponse(BaseModel):
    tables: list[dict[str, Any]]


class FeatureSearchRequest(BaseModel):
    filters: list[Filter] = []
    record_srn: str | None = None
    sort: str = "id"
    order: SortOrder = SortOrder.DESC
    cursor: str | None = None
    limit: int = Field(default=50, ge=1, le=100)


class FeatureSearchResponse(BaseModel):
    rows: list[dict[str, Any]]
    total: int
    cursor: str | None
    has_more: bool


# ── Routes ──


@router.post("/records")
async def search_records(
    body: RecordSearchRequest,
    handler: FromDishka[SearchRecordsHandler],
) -> RecordSearchResponse:
    """Search and filter published records."""
    result: SearchRecordsResult = await handler.run(
        SearchRecords(
            filters=body.filters,
            q=body.q,
            sort=body.sort,
            order=body.order,
            cursor=body.cursor,
            limit=body.limit,
        )
    )
    return RecordSearchResponse(
        results=result.results,
        total=result.total,
        cursor=result.cursor,
        has_more=result.has_more,
    )


@router.get("/features")
async def get_feature_catalog(
    handler: FromDishka[GetFeatureCatalogHandler],
) -> FeatureCatalogResponse:
    """List available feature tables with column schemas and record counts."""
    result: GetFeatureCatalogResult = await handler.run(GetFeatureCatalog())
    return FeatureCatalogResponse(tables=result.tables)


@router.post("/features/{hook_name}")
async def search_features(
    hook_name: str,
    body: FeatureSearchRequest,
    handler: FromDishka[SearchFeaturesHandler],
) -> FeatureSearchResponse:
    """Query and filter rows in a specific feature table."""
    result: SearchFeaturesResult = await handler.run(
        SearchFeatures(
            hook_name=hook_name,
            filters=body.filters,
            record_srn=body.record_srn,
            sort=body.sort,
            order=body.order,
            cursor=body.cursor,
            limit=body.limit,
        )
    )
    return FeatureSearchResponse(
        rows=result.rows,
        total=result.total,
        cursor=result.cursor,
        has_more=result.has_more,
    )
