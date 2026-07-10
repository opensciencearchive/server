"""Table-shaped tools: show_table, show_chart, fetch_page, sample_values (#162).

All four bind onto the domain's page-shaped view queries; ``show_chart`` is
the only one that wraps the payload (echoing the chart parameters around the
page for the widget)."""

from __future__ import annotations

from osa.application.api.mcp.models import (
    ChartData,
    FetchPageArgs,
    SampleValuesArgs,
    ShowChartArgs,
    ShowTableArgs,
)
from osa.application.api.mcp.tools.base import Tool, ToolSpec
from osa.domain.data.model.view import ColumnSample, TablePage
from osa.domain.data.query.view import (
    GetColumnSample,
    GetColumnSampleHandler,
    ReadTablePage,
    ReadTablePageHandler,
)


class ShowTable(Tool[ShowTableArgs, ReadTablePageHandler, TablePage]):
    spec = ToolSpec(
        name="show_table",
        title="Show table",
        description=(
            "Render an interactive, sortable, server-paginated table over a dataset's "
            "records table or one feature table, optionally filtered. Filters on the "
            "records table use `metadata.<field>` paths; filters on a feature table use "
            "`features.<table>.<column>` paths for that same table. After rendering, "
            "paging and re-sorting happen inside the widget with no further model calls."
        ),
        input_model=ShowTableArgs,
        resource_uri="ui://osa/table",
    )
    handler_type = ReadTablePageHandler

    async def run(self, args: ShowTableArgs) -> TablePage:
        return await self.handler.run(
            ReadTablePage(
                schema=args.schema,
                table=args.table,
                filter=args.filter,
                sort=args.sort,
                limit=args.limit,
            )
        )


class ShowChart(Tool[ShowChartArgs, ReadTablePageHandler, ChartData]):
    spec = ToolSpec(
        name="show_chart",
        title="Show chart",
        description=(
            "Render an interactive chart (line, scatter, or bar) from two columns of a "
            "records or feature table, with optional series grouping by a categorical "
            "column. Fetches one bounded page of raw rows (server page limit applies) "
            "and aggregates client-side; when the table exceeds the page limit the "
            "widget shows a truncation notice — the chart reflects only the fetched "
            "slice. Column names come from describe_dataset."
        ),
        input_model=ShowChartArgs,
        resource_uri="ui://osa/chart",
    )
    handler_type = ReadTablePageHandler

    async def run(self, args: ShowChartArgs) -> ChartData:
        page = await self.handler.run(
            ReadTablePage(
                schema=args.schema,
                table=args.table,
                filter=args.filter,
                sort=args.sort,
                limit=args.limit,
                require_columns=[c for c in (args.x, args.y, args.series) if c is not None],
            )
        )
        return ChartData(kind=args.kind, x=args.x, y=args.y, series=args.series, page=page)


class FetchPage(Tool[FetchPageArgs, ReadTablePageHandler, TablePage]):
    spec = ToolSpec(
        name="fetch_page",
        title="Fetch page",
        description=(
            "App-only: fetch the next/previous page (or a re-sorted first page) of a "
            "table for an already-rendered widget. Invoked by widgets via the host; "
            "never by the model."
        ),
        input_model=FetchPageArgs,
        app_only=True,
    )
    handler_type = ReadTablePageHandler

    async def run(self, args: FetchPageArgs) -> TablePage:
        return await self.handler.run(
            ReadTablePage(
                schema=args.schema,
                table=args.table,
                filter=args.filter,
                sort=args.sort,
                cursor=args.cursor,
                limit=args.limit,
            )
        )


class SampleValues(Tool[SampleValuesArgs, GetColumnSampleHandler, ColumnSample]):
    spec = ToolSpec(
        name="sample_values",
        title="Sample column values",
        description=(
            "App-only: a bounded, deduplicated sample of one column's non-null values, "
            "used by the filter panel to populate facet options."
        ),
        input_model=SampleValuesArgs,
        app_only=True,
    )
    handler_type = GetColumnSampleHandler

    async def run(self, args: SampleValuesArgs) -> ColumnSample:
        return await self.handler.run(
            GetColumnSample(
                schema=args.schema, table=args.table, column=args.column, limit=args.limit
            )
        )
