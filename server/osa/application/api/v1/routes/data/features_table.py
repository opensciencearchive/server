"""Feature-table routes — ``/data/{schema}/{feature}[.csv|.csv.gz]`` (US5).

Identical shape to the records table — same factory, same streaming/pagination
engine — but the path carries a ``{feature}`` segment and the handler builds a
``TableKind.FEATURE`` plan. An unknown feature 404s before any bytes (T090).
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from osa.application.api.v1.routes.data._limiter import POST_RATE_LIMIT, limiter
from osa.application.api.v1.routes.data._params import FilterRequestBody, parse_sort
from osa.application.api.v1.routes.data._streaming import build_table_response
from osa.application.api.v1.routes.data.formats import DataResponseFormat
from osa.application.api.v1.routes.data.tables import format_key, register_table_routes
from osa.domain.data.query.read_table import ReadFeatureTable, ReadFeatureTableHandler


def _make_get_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        schema: str,
        feature: str,
        handler: FromDishka[ReadFeatureTableHandler],
        cursor: str | None = None,
        limit: int = 50,
        sort: str | None = None,
    ) -> StreamingResponse:
        result = await handler.run(
            ReadFeatureTable(
                schema=schema,
                feature=feature,
                cursor=cursor,
                limit=limit,
                sort=parse_sort(sort),
                timeout=fmt.timeout,
            )
        )
        return await build_table_response(result.rows, fmt, result.columns, result.plan)

    return endpoint


def _make_post_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        request: Request,
        schema: str,
        feature: str,
        body: FilterRequestBody,
        handler: FromDishka[ReadFeatureTableHandler],
    ) -> StreamingResponse:
        result = await handler.run(
            ReadFeatureTable(
                schema=schema,
                feature=feature,
                filter=body.filter,
                cursor=body.cursor,
                limit=body.limit,
                sort=parse_sort(body.sort),
                timeout=fmt.timeout,
            )
        )
        return await build_table_response(result.rows, fmt, result.columns, result.plan)

    # Unique name before the limiter — see records_table._make_post_endpoint.
    endpoint.__name__ = f"feature_post_{format_key(fmt)}"
    endpoint.__qualname__ = endpoint.__name__
    return limiter.limit(POST_RATE_LIMIT)(endpoint)


def register(router: APIRouter) -> None:
    register_table_routes(
        router,
        "/{schema}/{feature}",
        _make_get_endpoint,
        _make_post_endpoint,
        "feature",
    )
