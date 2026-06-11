"""Records-table routes — ``/data/{schema}/records[.csv|.csv.gz]`` (US1 + US2).

Endpoint *builders* capture a :class:`DataResponseFormat` by closure; the
generic :func:`register_table_routes` factory registers the GET/POST × format
matrix from them. GET streams (or paginates JSON) with no body; POST takes a
``FilterExpr`` body and is rate-limited per IP. Every route applies the
format's ``statement_timeout`` and resolves the schema (404 before bytes).
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from sqlalchemy.ext.asyncio import AsyncSession

from osa.application.api.v1.routes.data._limiter import POST_RATE_LIMIT, limiter
from osa.application.api.v1.routes.data._params import FilterRequestBody, build_plan
from osa.application.api.v1.routes.data._runtime import apply_statement_timeout
from osa.application.api.v1.routes.data._streaming import build_table_response
from osa.application.api.v1.routes.data.tables import format_key, register_table_routes
from osa.config import Config
from osa.domain.data.model.format import DataResponseFormat
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService


def _make_get_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        schema: str,
        query_service: FromDishka[DataQueryService],
        catalog_service: FromDishka[DataCatalogService],
        session: FromDishka[AsyncSession],
        config: FromDishka[Config],
        cursor: str | None = None,
        limit: int = 50,
        sort: str | None = None,
    ) -> StreamingResponse:
        table = await catalog_service.resolve_table(schema, TableKind.RECORDS)
        await apply_statement_timeout(session, fmt)
        plan = build_plan(
            schema_id=table.schema_id,
            table_kind=TableKind.RECORDS,
            feature_name=None,
            filter_expr=None,
            cursor=cursor,
            limit=limit,
            max_limit=config.data.max_page_limit,
            sort=sort,
        )
        rows = query_service.stream_records(plan)
        return await build_table_response(rows, fmt, table.columns, plan)

    return endpoint


def _make_post_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        request: Request,
        schema: str,
        body: FilterRequestBody,
        query_service: FromDishka[DataQueryService],
        catalog_service: FromDishka[DataCatalogService],
        session: FromDishka[AsyncSession],
        config: FromDishka[Config],
    ) -> StreamingResponse:
        table = await catalog_service.resolve_table(schema, TableKind.RECORDS)
        await apply_statement_timeout(session, fmt)
        plan = build_plan(
            schema_id=table.schema_id,
            table_kind=TableKind.RECORDS,
            feature_name=None,
            filter_expr=body.filter,
            cursor=body.cursor,
            limit=body.limit,
            max_limit=config.data.max_page_limit,
            sort=body.sort,
        )
        rows = query_service.stream_records(plan)
        return await build_table_response(rows, fmt, table.columns, plan)

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
