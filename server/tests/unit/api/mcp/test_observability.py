"""Compact log summaries for MCP tool calls (#162).

The dispatcher logs one line per tool call; these pure helpers turn the typed
args and payload into short, operator-readable strings (no full filter trees,
no row dumps).
"""

from osa.application.api.mcp.models import ChartData, ShowChartArgs, ShowTableArgs
from osa.application.api.mcp.observability import summarize_args, summarize_result
from osa.domain.data.model.manifest import ColumnSpec, SchemaManifest
from osa.domain.data.model.view import (
    ColumnSample,
    DatasetList,
    DatasetSummary,
    Facet,
    FacetKind,
    FilterPanelData,
    TablePage,
    TableQuery,
)
from osa.domain.semantics.model.value import FieldType


def _table_page(rows: int, *, truncated: bool, cursor: str | None) -> TablePage:
    return TablePage(
        query=TableQuery(schema="alloy", table="tensile", limit=50),
        columns=[ColumnSpec(name="x", type=FieldType.NUMBER)],
        rows=[{"x": i} for i in range(rows)],
        next_cursor=cursor,
        truncated=truncated,
    )


class TestSummarizeArgs:
    def test_table_args_pick_key_fields(self):
        s = summarize_args(ShowTableArgs(schema="alloy", table="tensile", limit=25))
        assert "schema=alloy" in s
        assert "table=tensile" in s
        assert "limit=25" in s

    def test_flags_filter_presence_without_dumping_the_tree(self):
        s = summarize_args(
            ShowTableArgs(
                schema="alloy",
                filter={
                    "kind": "predicate",
                    "field": "metadata.supplier",
                    "op": "eq",
                    "value": "x",
                },
            )
        )
        assert "filter=yes" in s
        # The value must NOT be spelled out — summaries stay compact.
        assert "supplier" not in s

    def test_chart_args_include_axes(self):
        s = summarize_args(
            ShowChartArgs(schema="alloy", table="tensile", x="strain", y="stress", kind="line")
        )
        assert "x=strain" in s and "y=stress" in s and "kind=line" in s


class TestSummarizeResult:
    def test_table_page(self):
        s = summarize_result(_table_page(2, truncated=True, cursor="abc"))
        assert "2 rows" in s
        assert "truncated=True" in s
        assert "next_page=True" in s

    def test_table_page_last_page(self):
        s = summarize_result(_table_page(2, truncated=False, cursor=None))
        assert "next_page=False" in s

    def test_chart(self):
        page = _table_page(3, truncated=False, cursor=None)
        s = summarize_result(ChartData(kind="line", x="a", y="b", page=page))
        assert "line" in s and "3 rows" in s

    def test_dataset_list(self):
        payload = DatasetList(
            node_domain="localhost",
            datasets=[
                DatasetSummary(
                    id="alloy",
                    version="1.0.0",
                    srn="urn:osa:localhost:schema:alloy@1.0.0",
                    title="Alloy",
                    record_count=5,
                    feature_tables=[],
                )
            ],
        )
        assert "1 dataset" in summarize_result(payload)

    def test_filter_panel(self):
        payload = FilterPanelData(
            schema="alloy",
            table="records",
            facets=[
                Facet(field="metadata.x", label="x", kind=FacetKind.RANGE, type=FieldType.NUMBER)
            ],
        )
        assert "1 facet" in summarize_result(payload)

    def test_column_sample(self):
        assert "3 value" in summarize_result(
            ColumnSample(column="x", values=[1, 2, 3], truncated=False)
        )

    def test_manifest(self):
        payload = SchemaManifest(
            id="alloy",
            version="1.0.0",
            srn="urn:osa:localhost:schema:alloy@1.0.0",
            title="Alloy",
            fields=[],
            table_resources=[],
        )
        assert "alloy@1.0.0" in summarize_result(payload)

    def test_unknown_payload_falls_back_to_type_name(self):
        class Weird(TableQuery):
            pass

        s = summarize_result(Weird(schema="a", table="b", limit=1))
        assert "Weird" in s
