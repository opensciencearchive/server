"""Search API routes."""

from typing import Any

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from osa.domain.index.model.registry import IndexRegistry
from osa.sdk.index import QueryResult

router = APIRouter(
    prefix="/api/v1/search",
    tags=["search"],
    route_class=DishkaRoute,
)


class SearchResponse(BaseModel):
    """Search response model."""

    query: str
    index: str
    total: int
    results: list[dict[str, Any]]


@router.get("/{index_name}")
async def search_index(
    index_name: str,
    indexes: FromDishka[IndexRegistry],
    q: str = Query(..., description="Search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
) -> SearchResponse:
    """Search a specific index by name."""
    if not len(indexes):
        raise HTTPException(status_code=503, detail="No search indexes configured")

    backend = indexes.get(index_name)
    if not backend:
        raise HTTPException(
            status_code=404,
            detail=f"Index '{index_name}' not found. Available: {indexes.names()}",
        )

    result: QueryResult = await backend.query(q, limit=limit)

    return SearchResponse(
        query=q,
        index=index_name,
        total=len(result.hits),
        results=[
            {
                "id": hit.srn,
                "score": hit.score,
                "metadata": hit.metadata,
            }
            for hit in result.hits
        ],
    )


@router.get("/")
async def list_indexes(
    indexes: FromDishka[IndexRegistry],
) -> dict[str, list[str]]:
    """List available search indexes."""
    return {"indexes": indexes.names()}
