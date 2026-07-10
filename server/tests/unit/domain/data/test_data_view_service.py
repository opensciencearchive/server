"""DataViewService — payload-shaped reads for interactive consumers (#162).

Service-level tests with fakes at the same seams as the other data-domain
tests: a fake catalog service (resolution + manifests) and a fake query
service (row streams).
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from datetime import timedelta
from typing import Any

import pytest

from osa.domain.data.model.catalog import (
    CatalogEntry,
    NodeCatalog,
    TableResourceSummary,
)
from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    ResolvedTable,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import QueryPlan, TableKind
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.data.service.data_view import DataViewService
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import FeatureName
from osa.domain.shared.model.ids import RecordId, RecordRef
from osa.domain.shared.model.srn import RecordSRN, SchemaId

SCHEMA_ID = SchemaId.parse("alloy-sample@1.0.0")

FEATURE_COLUMNS = [
    ColumnSpec(name="id", type=FieldType.NUMBER),
    ColumnSpec(name="stress", type=FieldType.NUMBER, unit="MPa"),
    ColumnSpec(name="measured_at", type=FieldType.DATE),
]


def _manifest() -> SchemaManifest:
    return SchemaManifest(
        id="alloy-sample",
        version="1.0.0",
        srn="urn:osa:localhost:schema:alloy-sample@1.0.0",
        title="Alloy sample",
        fields=[FieldSpec(name="supplier", type=FieldType.TEXT)],
        table_resources=[
            TableResource(
                name="records", kind=TableKind.RECORDS, columns=[], row_count=7, formats=[""]
            ),
            TableResource(
                name="tensile_test",
                kind=TableKind.FEATURE,
                columns=[*IMPLICIT_FEATURE_COLUMN_SPECS, *FEATURE_COLUMNS[1:]],
                row_count=100,
                formats=[""],
            ),
        ],
    )


def _record() -> RecordSummary:
    return RecordSummary(
        id=RecordId("0198fa3-abc"),
        srn=RecordSRN.parse("urn:osa:localhost:rec:0198fa3-abc@1"),
        schema_id=SCHEMA_ID,
        version=1,
        metadata={"supplier": "acme"},
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


class FakeCatalogService:
    def __init__(self) -> None:
        self.manifest = _manifest()

    async def resolve_table(self, schema, table_kind, feature_name=None) -> ResolvedTable:
        return ResolvedTable(schema_id=SCHEMA_ID, columns=FEATURE_COLUMNS)

    async def resolve_schema(self, raw: str) -> SchemaId:
        return SCHEMA_ID

    async def get_node_catalog(self) -> NodeCatalog:
        return NodeCatalog(
            node_domain="localhost",
            schemas=[
                CatalogEntry(
                    id="alloy-sample",
                    version="1.0.0",
                    srn="urn:osa:localhost:schema:alloy-sample@1.0.0",
                    table_resources=[TableResourceSummary(name="records", kind=TableKind.RECORDS)],
                )
            ],
        )

    async def get_schema_manifest(self, schema_id: SchemaId) -> SchemaManifest:
        return self.manifest

    async def get_record_by_id(self, id: RecordId, version: int | None) -> RecordSummary:
        return _record()


class FakeQueryService:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items
        self.received: tuple[QueryPlan, timedelta | None] | None = None

    async def _rows(self) -> AsyncIterator[Mapping[str, Any]]:
        for item in self.items:
            yield item

    def stream_records(self, plan: QueryPlan, timeout: timedelta | None = None):
        self.received = (plan, timeout)
        return self._rows()

    def stream_features(self, plan: QueryPlan, timeout: timedelta | None = None):
        self.received = (plan, timeout)
        return self._rows()


@dataclass
class FakeDataConfig:
    max_filter_depth: int = 10
    max_predicates: int = 200
    max_feature_joins: int = 10
    max_page_limit: int = 1000


@dataclass
class FakeConfig:
    data: FakeDataConfig = field(default_factory=FakeDataConfig)


def _service(items: list[dict[str, Any]] | None = None) -> DataViewService:
    return DataViewService(
        catalog_service=FakeCatalogService(),
        query_service=FakeQueryService(items or []),
        config=FakeConfig(),
    )


def _feature_row(i: int) -> dict[str, Any]:
    return {
        "id": i,
        "stress": 900.0 + i,
        "measured_at": datetime(2026, 1, i + 1, tzinfo=timezone.utc),
        "not_in_columns": "dropped",
    }


class TestPage:
    async def test_rows_projected_to_declared_columns_and_json_safe(self):
        service = _service([_feature_row(0)])
        page = await service.page(schema="alloy-sample", table="tensile_test", limit=10)
        row = page.rows[0]
        assert set(row.keys()) == {"id", "stress", "measured_at"}
        # datetime rendered as a string, not a datetime object
        assert isinstance(row["measured_at"], str)
        assert row["measured_at"].startswith("2026-01-01")

    async def test_truncation_and_cursor(self):
        service = _service([_feature_row(i) for i in range(3)])
        page = await service.page(schema="alloy-sample", table="tensile_test", limit=2)
        assert page.truncated is True
        assert page.next_cursor is not None
        assert len(page.rows) == 2

    async def test_query_echo_carries_fetch_page_context(self):
        service = _service([])
        page = await service.page(schema="alloy-sample", table="tensile_test", limit=5)
        assert page.query.schema == "alloy-sample"
        assert page.query.table == "tensile_test"
        assert page.query.limit == 5

    async def test_limit_clamped_to_config_max(self):
        service = _service([])
        page = await service.page(schema="alloy-sample", table="tensile_test", limit=999999)
        assert page.query.limit == FakeDataConfig().max_page_limit

    async def test_unknown_required_column_lists_available(self):
        service = _service([])
        with pytest.raises(ValidationError, match="stress"):
            await service.page(
                schema="alloy-sample",
                table="tensile_test",
                limit=10,
                require_columns=["bogus"],
            )


class TestColumnSample:
    async def test_dedupes_and_drops_nulls(self):
        rows = [
            {"id": 1, "stress": 900.0, "measured_at": None},
            {"id": 2, "stress": 900.0, "measured_at": None},
            {"id": 3, "stress": None, "measured_at": None},
            {"id": 4, "stress": 905.0, "measured_at": None},
        ]
        service = _service(rows)
        sample = await service.column_sample(
            schema="alloy-sample", table="tensile_test", column="stress", limit=10
        )
        assert sample.values == [900.0, 905.0]
        assert sample.truncated is False


class TestDatasetList:
    async def test_counts_and_feature_tables_from_manifest(self):
        service = _service([])
        listing = await service.dataset_list()
        assert listing.node_domain == "localhost"
        (dataset,) = listing.datasets
        assert dataset.title == "Alloy sample"
        assert dataset.record_count == 7
        assert dataset.feature_tables == [FeatureName("tensile_test")]


class TestRecordDetail:
    async def test_record_with_joinable_feature_tables(self):
        service = _service([])
        detail = await service.record_detail(RecordRef(id=RecordId("0198fa3-abc")))
        assert detail.record.metadata == {"supplier": "acme"}
        assert detail.feature_tables == [FeatureName("tensile_test")]
        assert detail.schema == "alloy-sample"
