"""Search API routes."""

from typing import Any

from dishka.integrations.fastapi import DishkaRoute, FromDishka
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from osa.sdk.index import QueryResult, StorageBackend

router = APIRouter(
    prefix="/api/v1/search",
    tags=["search"],
    route_class=DishkaRoute,
)


class SearchResponse(BaseModel):
    """Search response model."""

    query: str
    backend: str
    total: int
    results: list[dict[str, Any]]


@router.get("/vector")
async def vector_search(
    backend: FromDishka[StorageBackend],
    q: str = Query(..., description="Natural language search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
) -> SearchResponse:
    """Search using vector similarity."""
    try:
        result: QueryResult = await backend.query(q, limit=limit)

        return SearchResponse(
            query=q,
            backend="vector",
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get("/keyword")
async def keyword_search(
    q: str = Query(..., description="Keyword search query"),
    limit: int = Query(20, ge=1, le=100, description="Maximum number of results"),
) -> SearchResponse:
    """Search using keyword matching (future)."""
    raise HTTPException(status_code=501, detail="Keyword search not yet implemented")
