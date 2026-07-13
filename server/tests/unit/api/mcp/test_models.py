"""Tool input models — the MCP tool argument boundary (#162).

The models reuse the domain's ``FilterExpr``/``SortSpec`` Pydantic types
wholesale, so a malformed filter (bad dotted path, unknown operator) is
rejected at the tool boundary with the exact same rules as the REST surface.
"""

import pytest
from pydantic import ValidationError

from osa.application.api.mcp.models import (
    DescribeDatasetArgs,
    FetchPageArgs,
    SampleValuesArgs,
    ShowChartArgs,
    ShowRecordArgs,
    ShowTableArgs,
)
from osa.domain.data.model.filter import Predicate
from osa.domain.data.model.query_plan import SortDirection


class TestShowTableArgs:
    def test_defaults_to_records_table(self):
        args = ShowTableArgs(schema="alloy-sample")
        assert args.table == "records"
        assert args.filter is None
        assert args.sort == []

    def test_accepts_wire_form_filter(self):
        args = ShowTableArgs(
            schema="alloy-sample",
            table="tensile_test",
            filter={
                "kind": "predicate",
                "field": "features.tensile_test.stress",
                "op": "gte",
                "value": 900,
            },
        )
        assert isinstance(args.filter, Predicate)
        assert args.filter.field.dotted() == "features.tensile_test.stress"

    def test_rejects_malformed_filter_path(self):
        with pytest.raises(ValidationError):
            ShowTableArgs(
                schema="alloy-sample",
                filter={"kind": "predicate", "field": "bogus.path", "op": "eq", "value": 1},
            )

    def test_rejects_unknown_operator(self):
        with pytest.raises(ValidationError):
            ShowTableArgs(
                schema="alloy-sample",
                filter={
                    "kind": "predicate",
                    "field": "metadata.supplier",
                    "op": "like",
                    "value": "x",
                },
            )

    def test_accepts_sort_specs(self):
        args = ShowTableArgs(
            schema="alloy-sample",
            sort=[{"column": "created_at", "direction": "desc"}],
        )
        assert args.sort[0].column == "created_at"
        assert args.sort[0].direction == SortDirection.DESC

    def test_rejects_invalid_table_name(self):
        # Table names are PG-identifier-shaped; anything else must fail before
        # reaching a handler.
        with pytest.raises(ValidationError):
            ShowTableArgs(schema="alloy-sample", table="Robert'); DROP TABLE--")

    def test_json_schema_is_generated_with_filter_defs(self):
        # The host constrains the model's arguments with this schema — it must
        # embed the FilterExpr union, not an opaque Any.
        schema = ShowTableArgs.model_json_schema()
        assert "$defs" in schema
        assert "Predicate" in schema["$defs"]


class TestShowChartArgs:
    def test_requires_axes_and_kind(self):
        args = ShowChartArgs(
            schema="alloy-sample", table="tensile_test", x="strain", y="stress", kind="line"
        )
        assert args.kind == "line"

    def test_rejects_unknown_chart_kind(self):
        with pytest.raises(ValidationError):
            ShowChartArgs(schema="alloy-sample", table="tensile_test", x="a", y="b", kind="pie")


class TestOtherArgs:
    def test_show_record_parses_versioned_ref(self):
        args = ShowRecordArgs(id="0198fa3-abc@2")
        assert args.record_ref.version == 2

    def test_show_record_bare_id_means_latest(self):
        args = ShowRecordArgs(id="0198fa3-abc")
        assert args.record_ref.version is None

    def test_fetch_page_extends_show_table_with_cursor(self):
        args = FetchPageArgs(schema="alloy-sample", cursor="abc123")
        assert args.cursor == "abc123"
        assert args.table == "records"

    def test_sample_values_bounded_limit(self):
        args = SampleValuesArgs(schema="alloy-sample", table="records", column="supplier")
        assert args.limit > 0

    def test_describe_dataset(self):
        assert DescribeDatasetArgs(schema="alloy-sample").schema == "alloy-sample"
