"""Table-read query handlers — one entry point per ``/data/`` table request.

The handler owns everything between HTTP parsing and row streaming: table
resolution (404 before bytes), plan construction with the config-clamped page
limit, and delegation to the query service. Routes only translate HTTP
primitives into the Query DTO and wrap the result in a response.

``__auth__ = public()`` is the seam where the deferred private-schema auth
model will land.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum
from typing import Any

from pydantic import Field

from osa.config import Config
from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.model.query_plan import (
    BoundedPage,
    FullStream,
    PaginationCursor,
    QueryPlan,
    SortSpec,
    TableKind,
)
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.ids import FeatureName
from osa.domain.shared.query import Query, QueryHandler


class ReadMode(StrEnum):
    """How much of the table a read may return — chosen by the response format.

    ``PAGE`` is the default everywhere; ``STREAM`` (the whole table) must be
    named explicitly and only the CSV/gzip dump formats do (#219 phase 2).
    """

    PAGE = "page"
    STREAM = "stream"


class ReadRecordsTable(Query):
    schema: str  # URL segment: ``<id>`` or ``<id>@<semver>``
    filter: FilterExpr | None = None
    cursor: str | None = None
    limit: int = 50
    sort: list[SortSpec] = Field(default_factory=list)
    mode: ReadMode = ReadMode.PAGE
    timeout: timedelta | None = None  # execution budget chosen by the response format


class ReadFeatureTable(ReadRecordsTable):
    feature: FeatureName


@dataclass
class TableRead:
    """A resolved table read: the plan (pagination contract), the column
    schema (wire order), and the lazily-evaluated row stream."""

    plan: QueryPlan
    columns: list[ColumnSpec]
    rows: AsyncIterator[Mapping[str, Any]]


def _pagination(cmd: ReadRecordsTable, config: Config) -> BoundedPage | FullStream:
    if cmd.mode == ReadMode.STREAM:
        return FullStream()
    return BoundedPage.clamped(
        cursor=PaginationCursor(value=cmd.cursor) if cmd.cursor else None,
        limit=cmd.limit,
        max_limit=config.data.max_page_limit,
    )


class ReadRecordsTableHandler(QueryHandler[ReadRecordsTable, TableRead]):
    __auth__ = public()
    catalog_service: DataCatalogService
    query_service: DataQueryService
    config: Config

    async def run(self, cmd: ReadRecordsTable) -> TableRead:
        table = await self.catalog_service.resolve_table(cmd.schema, TableKind.RECORDS)
        plan = QueryPlan(
            schema_id=table.schema_id,
            table_kind=TableKind.RECORDS,
            filter=cmd.filter,
            pagination=_pagination(cmd, self.config),
            sort=cmd.sort,
        )
        rows = self.query_service.stream_records(plan, cmd.timeout)
        return TableRead(plan=plan, columns=table.columns, rows=rows)


class ReadFeatureTableHandler(QueryHandler[ReadFeatureTable, TableRead]):
    __auth__ = public()
    catalog_service: DataCatalogService
    query_service: DataQueryService
    config: Config

    async def run(self, cmd: ReadFeatureTable) -> TableRead:
        table = await self.catalog_service.resolve_table(
            cmd.schema, TableKind.FEATURE, feature_name=cmd.feature
        )
        plan = QueryPlan(
            schema_id=table.schema_id,
            table_kind=TableKind.FEATURE,
            feature_name=cmd.feature,
            filter=cmd.filter,
            pagination=_pagination(cmd, self.config),
            sort=cmd.sort,
        )
        rows = self.query_service.stream_features(plan, cmd.timeout)
        return TableRead(plan=plan, columns=table.columns, rows=rows)
