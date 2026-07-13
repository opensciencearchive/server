"""View query handlers — payload-shaped reads for interactive consumers (#162).

One entry point per view projection (table page, dataset list, record detail,
filter panel, column sample), all delegating to :class:`DataViewService`.
``__auth__ = public()`` throughout: these are projections over published data,
same posture as the rest of the ``/data/`` surface.
"""

from __future__ import annotations

from pydantic import Field

from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.query_plan import SortSpec
from osa.domain.data.model.view import (
    RECORDS_TABLE,
    ColumnSample,
    DatasetList,
    FilterPanelData,
    RecordDetailData,
    TablePage,
)
from osa.domain.data.service.data_view import DataViewService
from osa.domain.shared.authorization.gate import public
from osa.domain.shared.model.ids import RecordRef
from osa.domain.shared.query import Query, QueryHandler

# Table names are PG-identifier-shaped ("records" or a feature-table name);
# reject anything else before it reaches a resolver.
TABLE_PATTERN = r"^[a-z][a-z0-9_]*$"


class ReadTablePage(Query):
    schema: str  # ``<id>`` or ``<id>@<semver>``
    table: str = Field(default=RECORDS_TABLE, pattern=TABLE_PATTERN)
    filter: FilterExpr | None = None
    sort: list[SortSpec] = Field(default_factory=list)
    cursor: str | None = None
    limit: int = 50
    # Columns the consumer is about to address (chart axes, a sampled facet
    # column); unknown names fail with the available columns listed.
    require_columns: list[str] = Field(default_factory=list)


class ReadTablePageHandler(QueryHandler[ReadTablePage, TablePage]):
    __auth__ = public()
    service: DataViewService

    async def run(self, cmd: ReadTablePage) -> TablePage:
        return await self.service.page(
            schema=cmd.schema,
            table=cmd.table,
            filter=cmd.filter,
            sort=cmd.sort,
            cursor=cmd.cursor,
            limit=cmd.limit,
            require_columns=cmd.require_columns,
        )


class GetDatasetList(Query):
    pass


class GetDatasetListHandler(QueryHandler[GetDatasetList, DatasetList]):
    __auth__ = public()
    service: DataViewService

    async def run(self, cmd: GetDatasetList) -> DatasetList:
        return await self.service.dataset_list()


class GetRecordDetail(Query):
    ref: RecordRef


class GetRecordDetailHandler(QueryHandler[GetRecordDetail, RecordDetailData]):
    __auth__ = public()
    service: DataViewService

    async def run(self, cmd: GetRecordDetail) -> RecordDetailData:
        return await self.service.record_detail(cmd.ref)


class GetFilterPanel(Query):
    schema: str
    table: str = Field(default=RECORDS_TABLE, pattern=TABLE_PATTERN)


class GetFilterPanelHandler(QueryHandler[GetFilterPanel, FilterPanelData]):
    __auth__ = public()
    service: DataViewService

    async def run(self, cmd: GetFilterPanel) -> FilterPanelData:
        return await self.service.filter_panel(cmd.schema, cmd.table)


class GetColumnSample(Query):
    schema: str
    table: str = Field(default=RECORDS_TABLE, pattern=TABLE_PATTERN)
    column: str = Field(pattern=TABLE_PATTERN)
    limit: int = Field(default=100, ge=1)


class GetColumnSampleHandler(QueryHandler[GetColumnSample, ColumnSample]):
    __auth__ = public()
    service: DataViewService

    async def run(self, cmd: GetColumnSample) -> ColumnSample:
        return await self.service.column_sample(
            schema=cmd.schema, table=cmd.table, column=cmd.column, limit=cmd.limit
        )
