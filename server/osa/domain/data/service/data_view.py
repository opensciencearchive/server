"""DataViewService — bounded, payload-shaped reads for interactive consumers (#162).

Composes the existing catalog + query services into the view projections in
``model/view.py``: one JSON-safe table page, the dataset list with counts, a
record with its joinable feature tables, manifest-derived facets, and a
bounded column sample. No SQL and no new port methods — everything rides the
same stores, bounds, and clamps as the REST ``/data/`` surface.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any, ClassVar

from osa.config import Config
from osa.domain.data.model.filter import FilterExpr
from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.model.query_plan import (
    PaginationCursor,
    PaginationParams,
    QueryPlan,
    SortSpec,
    TableKind,
)
from osa.domain.data.model.view import (
    RECORDS_TABLE,
    ColumnSample,
    DatasetList,
    DatasetSummary,
    FilterPanelData,
    RecordDetailData,
    TablePage,
    TableQuery,
)
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.data.service.data_query import DataQueryService
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import FeatureName
from osa.domain.shared.model.ids import RecordRef
from osa.domain.shared.model.srn import SchemaId
from osa.domain.shared.service import Service


class DataViewService(Service):
    catalog_service: DataCatalogService
    query_service: DataQueryService
    config: Config

    # View reads are always paginated — same statement budget as the REST
    # paginated-JSON format.
    PAGE_TIMEOUT: ClassVar[timedelta] = timedelta(seconds=30)

    async def page(
        self,
        *,
        schema: str,
        table: str,
        filter: FilterExpr | None = None,
        sort: Sequence[SortSpec] = (),
        cursor: str | None = None,
        limit: int,
        require_columns: Sequence[str] = (),
    ) -> TablePage:
        """One bounded, JSON-safe page of the records table or a feature table.

        ``require_columns`` names columns the consumer is about to address
        (chart axes, a sampled facet column); unknown names raise
        ``ValidationError`` listing what the table actually offers.
        """
        table_kind = TableKind.RECORDS if table == RECORDS_TABLE else TableKind.FEATURE
        feature_name = None if table_kind == TableKind.RECORDS else FeatureName(table)
        resolved = await self.catalog_service.resolve_table(
            schema, table_kind, feature_name=feature_name
        )
        self._check_required_columns(resolved.columns, require_columns)
        plan = QueryPlan(
            schema_id=resolved.schema_id,
            table_kind=table_kind,
            feature_name=feature_name,
            filter=filter,
            pagination=PaginationParams.clamped(
                cursor=PaginationCursor(value=cursor) if cursor else None,
                limit=limit,
                max_limit=self.config.data.max_page_limit,
            ),
            sort=list(sort),
        )
        if table_kind == TableKind.RECORDS:
            rows = self.query_service.stream_records(plan, self.PAGE_TIMEOUT)
        else:
            rows = self.query_service.stream_features(plan, self.PAGE_TIMEOUT)
        slice_ = await plan.take_page(rows)
        return TablePage(
            query=TableQuery(
                schema=schema,
                table=table,
                filter=filter,
                sort=list(sort),
                limit=plan.pagination.limit,
            ),
            columns=resolved.columns,
            rows=[self._render_row(row, resolved.columns) for row in slice_.rows],
            next_cursor=slice_.next_cursor,
            truncated=slice_.truncated,
        )

    async def dataset_list(self) -> DatasetList:
        """Every published schema with its record count and feature tables.

        Row counts come from the per-schema manifests (live counts, matching
        the SKILL.md surface's posture).
        """
        catalog = await self.catalog_service.get_node_catalog()
        datasets: list[DatasetSummary] = []
        for entry in catalog.schemas:
            manifest = await self.catalog_service.get_schema_manifest(
                SchemaId.parse(f"{entry.id}@{entry.version}")
            )
            records = next((r for r in manifest.table_resources if r.name == RECORDS_TABLE), None)
            datasets.append(
                DatasetSummary(
                    id=manifest.id,
                    version=manifest.version,
                    srn=manifest.srn,
                    title=manifest.title,
                    record_count=records.row_count if records else 0,
                    feature_tables=[
                        FeatureName(r.name)
                        for r in manifest.table_resources
                        if r.kind == TableKind.FEATURE
                    ],
                )
            )
        return DatasetList(node_domain=catalog.node_domain, datasets=datasets)

    async def record_detail(self, ref: RecordRef) -> RecordDetailData:
        """One record plus the feature tables a detail view can join on."""
        record = await self.catalog_service.get_record_by_id(ref.id, ref.version)
        manifest = await self.catalog_service.get_schema_manifest(record.schema_id)
        return RecordDetailData(
            record=record,
            schema=manifest.id,
            feature_tables=[
                FeatureName(r.name) for r in manifest.table_resources if r.kind == TableKind.FEATURE
            ],
        )

    async def filter_panel(self, schema: str, table: str) -> FilterPanelData:
        """Manifest-derived facet controls for one table."""
        schema_id = await self.catalog_service.resolve_schema(schema)
        manifest = await self.catalog_service.get_schema_manifest(schema_id)
        return FilterPanelData.from_manifest(manifest, table)

    async def column_sample(
        self, *, schema: str, table: str, column: str, limit: int
    ) -> ColumnSample:
        """Bounded, deduped non-null scalar values of one column.

        There is no DISTINCT endpoint by design — this reads one page and
        dedupes, so it inherits the same bounds as every other read.
        """
        page = await self.page(schema=schema, table=table, limit=limit, require_columns=[column])
        seen: dict[str | int | float | bool, None] = {}
        for row in page.rows:
            value = row.get(column)
            if isinstance(value, (str, int, float, bool)):
                seen.setdefault(value)
        return ColumnSample(column=column, values=list(seen.keys()), truncated=page.truncated)

    # ------------------------------------------------------------------ #

    @staticmethod
    def _render_row(row: Mapping[str, Any], columns: Sequence[ColumnSpec]) -> dict[str, Any]:
        """Project onto the declared columns and render values JSON-safe.

        Same ``default=str`` posture as the JSON serializer: a datetime or
        Decimal arrives as its string form, never as a Python object.
        """
        projected = {col.name: row.get(col.name) for col in columns}
        return json.loads(json.dumps(projected, default=str))

    @staticmethod
    def _check_required_columns(columns: Sequence[ColumnSpec], required: Sequence[str]) -> None:
        available = {c.name for c in columns}
        unknown = [name for name in required if name not in available]
        if unknown:
            raise ValidationError(
                f"Unknown column(s) {unknown} on this table. "
                f"Available columns: {sorted(available)}.",
                field="columns",
                code="unknown_column",
            )
