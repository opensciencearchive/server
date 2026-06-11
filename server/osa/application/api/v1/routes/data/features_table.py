"""Feature-table routes — ``/data/{schema}/{feature}[.csv|.csv.gz]`` (US5).

Identical shape to the records table — same factory, same streaming/pagination
engine — but the path carries a ``{feature}`` segment and the plan is a
``TableKind.FEATURE`` plan. The feature must be a table resource on the resolved
schema's manifest; an unknown feature 404s before any bytes (T090).
"""

from __future__ import annotations

from collections.abc import Sequence

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
from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.srn import SchemaId


async def _feature_columns(
    catalog: DataCatalogService, schema: str, feature: str
) -> tuple[Sequence[ColumnSpec], SchemaId]:
    """Resolve schema + feature (404 if unknown/reserved) → feature columns + SchemaId."""
    schema_id = await catalog.resolve_schema(schema)
    manifest = await catalog.get_schema_manifest(schema_id)
    resource = next(
        (
            tr
            for tr in manifest.table_resources
            if tr.name == feature and tr.kind == TableKind.FEATURE
        ),
        None,
    )
    if resource is None:
        raise NotFoundError(
            f"No feature table '{feature}' on schema '{schema}'. "
            f"See /api/v1/data/{schema_id.render()} for its table resources.",
            code="feature_not_found",
        )
    return resource.columns, schema_id


def _make_get_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        schema: str,
        feature: str,
        query_service: FromDishka[DataQueryService],
        catalog_service: FromDishka[DataCatalogService],
        session: FromDishka[AsyncSession],
        config: FromDishka[Config],
        cursor: str | None = None,
        limit: int = 50,
        sort: str | None = None,
    ) -> StreamingResponse:
        columns, schema_id = await _feature_columns(catalog_service, schema, feature)
        await apply_statement_timeout(session, fmt)
        plan = build_plan(
            schema_id=schema_id,
            table_kind=TableKind.FEATURE,
            feature_name=feature,
            filter_expr=None,
            cursor=cursor,
            limit=limit,
            max_limit=config.data.max_page_limit,
            sort=sort,
        )
        rows = query_service.stream_features(plan)
        return await build_table_response(rows, fmt, columns, plan)

    return endpoint


def _make_post_endpoint(fmt: DataResponseFormat):
    async def endpoint(
        request: Request,
        schema: str,
        feature: str,
        body: FilterRequestBody,
        query_service: FromDishka[DataQueryService],
        catalog_service: FromDishka[DataCatalogService],
        session: FromDishka[AsyncSession],
        config: FromDishka[Config],
    ) -> StreamingResponse:
        columns, schema_id = await _feature_columns(catalog_service, schema, feature)
        await apply_statement_timeout(session, fmt)
        plan = build_plan(
            schema_id=schema_id,
            table_kind=TableKind.FEATURE,
            feature_name=feature,
            filter_expr=body.filter,
            cursor=body.cursor,
            limit=body.limit,
            max_limit=config.data.max_page_limit,
            sort=body.sort,
        )
        rows = query_service.stream_features(plan)
        return await build_table_response(rows, fmt, columns, plan)

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
