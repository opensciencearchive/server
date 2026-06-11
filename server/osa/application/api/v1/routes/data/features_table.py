"""Feature-table routes — ``/data/{schema}/{feature}[.csv|.csv.gz]`` (US5).

Identical shape to the records table — same factory, same streaming/pagination
engine — but the path carries a ``{feature}`` segment and the plan is a
``TableKind.FEATURE`` plan. The feature must be a table resource on the resolved
schema's manifest; an unknown feature 404s before any bytes (T090).
"""

from __future__ import annotations

from dishka.integrations.fastapi import FromDishka
from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from osa.application.api.v1.routes.data._limiter import POST_RATE_LIMIT, limiter
from osa.application.api.v1.routes.data._params import FilterRequestBody, build_plan
from osa.application.api.v1.routes.data._streaming import build_table_response
from osa.application.api.v1.routes.data.tables import format_key, register_table_routes
from osa.config import Config
from osa.application.api.v1.routes.data.formats import DataResponseFormat
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService


def _make_get_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        schema: str,
        feature: str,
        query_service: FromDishka[DataQueryService],
        catalog_service: FromDishka[DataCatalogService],
        config: FromDishka[Config],
        cursor: str | None = None,
        limit: int = 50,
        sort: str | None = None,
    ) -> StreamingResponse:
        table = await catalog_service.resolve_table(schema, TableKind.FEATURE, feature_name=feature)
        plan = build_plan(
            schema_id=table.schema_id,
            table_kind=TableKind.FEATURE,
            feature_name=feature,
            filter_expr=None,
            cursor=cursor,
            limit=limit,
            max_limit=config.data.max_page_limit,
            sort=sort,
        )
        rows = query_service.stream_features(plan, timeout=fmt.timeout)
        return await build_table_response(rows, fmt, table.columns, plan)

    return endpoint


def _make_post_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        request: Request,
        schema: str,
        feature: str,
        body: FilterRequestBody,
        query_service: FromDishka[DataQueryService],
        catalog_service: FromDishka[DataCatalogService],
        config: FromDishka[Config],
    ) -> StreamingResponse:
        table = await catalog_service.resolve_table(schema, TableKind.FEATURE, feature_name=feature)
        plan = build_plan(
            schema_id=table.schema_id,
            table_kind=TableKind.FEATURE,
            feature_name=feature,
            filter_expr=body.filter,
            cursor=body.cursor,
            limit=body.limit,
            max_limit=config.data.max_page_limit,
            sort=body.sort,
        )
        rows = query_service.stream_features(plan, timeout=fmt.timeout)
        return await build_table_response(rows, fmt, table.columns, plan)

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
