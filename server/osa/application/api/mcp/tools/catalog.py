"""Catalog-shaped tools: list_datasets, describe_dataset, show_record,
show_filter_panel (#162). Each is a thin binding onto one domain query."""

from __future__ import annotations

from osa.application.api.mcp.models import (
    DescribeDatasetArgs,
    ListDatasetsArgs,
    ShowFilterPanelArgs,
    ShowRecordArgs,
)
from osa.application.api.mcp.tools.base import Tool, ToolSpec
from osa.domain.data.model.manifest import SchemaManifest
from osa.domain.data.model.view import DatasetList, FilterPanelData, RecordDetailData
from osa.domain.data.query.catalog import GetSchemaManifest, GetSchemaManifestHandler
from osa.domain.data.query.view import (
    GetDatasetList,
    GetDatasetListHandler,
    GetFilterPanel,
    GetFilterPanelHandler,
    GetRecordDetail,
    GetRecordDetailHandler,
)


class ListDatasets(Tool[ListDatasetsArgs, GetDatasetListHandler, DatasetList]):
    spec = ToolSpec(
        name="list_datasets",
        title="List datasets",
        description=(
            "List every published dataset (schema) on this archive node, with record "
            "counts and available feature tables. Start here to discover what data "
            "exists. Renders a dataset-overview widget."
        ),
        input_model=ListDatasetsArgs,
        resource_uri="ui://osa/dataset-overview",
    )
    handler_type = GetDatasetListHandler

    async def run(self, args: ListDatasetsArgs) -> DatasetList:
        return await self.handler.run(GetDatasetList())


class DescribeDataset(Tool[DescribeDatasetArgs, GetSchemaManifestHandler, SchemaManifest]):
    spec = ToolSpec(
        name="describe_dataset",
        title="Describe dataset",
        description=(
            "Full machine-readable manifest for one dataset: typed metadata fields and "
            "every table resource (the records table plus feature tables) with column "
            "types, units, descriptions, examples, and row counts. Call this before "
            "composing filters, tables, or charts — filter paths and chart axes come "
            "from these field/column names."
        ),
        input_model=DescribeDatasetArgs,
    )
    handler_type = GetSchemaManifestHandler

    async def run(self, args: DescribeDatasetArgs) -> SchemaManifest:
        return await self.handler.run(GetSchemaManifest(schema=args.schema))


class ShowRecord(Tool[ShowRecordArgs, GetRecordDetailHandler, RecordDetailData]):
    spec = ToolSpec(
        name="show_record",
        title="Show record",
        description=(
            "Fetch one published record by its id (optionally `id@version`) and render "
            "a record-detail card: metadata plus, on demand, joined feature rows with "
            "their run provenance."
        ),
        input_model=ShowRecordArgs,
        resource_uri="ui://osa/record",
    )
    handler_type = GetRecordDetailHandler

    async def run(self, args: ShowRecordArgs) -> RecordDetailData:
        return await self.handler.run(GetRecordDetail(ref=args.record_ref))


class ShowFilterPanel(Tool[ShowFilterPanelArgs, GetFilterPanelHandler, FilterPanelData]):
    spec = ToolSpec(
        name="show_filter_panel",
        title="Show filter panel",
        description=(
            "Render a faceted filter panel derived from the dataset manifest: ranges "
            "for number/date fields, selects for boolean/term fields, text search for "
            "text/url fields. The panel emits a filter expression the table and chart "
            "tools accept."
        ),
        input_model=ShowFilterPanelArgs,
        resource_uri="ui://osa/filter-panel",
    )
    handler_type = GetFilterPanelHandler

    async def run(self, args: ShowFilterPanelArgs) -> FilterPanelData:
        return await self.handler.run(GetFilterPanel(schema=args.schema, table=args.table))
