"""Records-table routes — ``/data/{schema}/records[.csv|.csv.gz]`` (US1 + US2).

Endpoint *builders* capture a :class:`DataResponseFormat` by closure; the
generic :func:`register_table_routes` factory registers the GET/POST × format
matrix from them. GET streams (or paginates JSON) with no body; POST takes a
``FilterExpr`` body and is rate-limited per IP. Everything between HTTP and
the row stream — table resolution, plan construction, limit clamping — lives
in :class:`ReadRecordsTableHandler`.
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
from osa.domain.data.query.read_table import ReadRecordsTable, ReadRecordsTableHandler


def _make_get_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        schema: str,
        handler: FromDishka[ReadRecordsTableHandler],
        cursor: str | None = None,
        limit: int = 50,
        sort: str | None = None,
    ) -> StreamingResponse:
        result = await handler.run(
            ReadRecordsTable(
                schema=schema,
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
        body: FilterRequestBody,
        handler: FromDishka[ReadRecordsTableHandler],
    ) -> StreamingResponse:
        result = await handler.run(
            ReadRecordsTable(
                schema=schema,
                filter=body.filter,
                cursor=body.cursor,
                limit=body.limit,
                sort=parse_sort(body.sort),
                timeout=fmt.timeout,
            )
        )
        return await build_table_response(result.rows, fmt, result.columns, result.plan)

    # slowapi scopes a rate limit by the decorated function's ``module.__name__``
    # captured at decoration time. These builder closures are all named
    # ``endpoint``, so name each uniquely BEFORE applying the limiter, or every
    # POST route would collapse into a single shared limit bucket.
    endpoint.__name__ = f"records_post_{format_key(fmt)}"
    endpoint.__qualname__ = endpoint.__name__
    return limiter.limit(POST_RATE_LIMIT)(endpoint)


def register(router: APIRouter) -> None:
    register_table_routes(
        router,
        "/{schema}/records",
        _make_get_endpoint,
        _make_post_endpoint,
        "records",
    )
