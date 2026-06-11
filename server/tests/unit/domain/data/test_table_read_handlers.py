"""Data-domain query handlers — the route's single entry point per request.

Restores the documented Router → QueryHandler → Service layering on the /data/
surface (and with it the __auth__ seam the deferred private-schema auth model
will need). Handlers own plan construction: table resolution, limit clamping
against config, and default sorts — routes only parse HTTP.
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest

from osa.domain.data.model.manifest import ColumnSpec, ResolvedTable
from osa.domain.data.model.query_plan import (
    QueryPlan,
    SortDirection,
    SortSpec,
    TableKind,
)
from osa.domain.data.query.read_table import (
    ReadFeatureTable,
    ReadFeatureTableHandler,
    ReadRecordsTable,
    ReadRecordsTableHandler,
)
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.model.srn import SchemaId

SCHEMA_ID = SchemaId.parse("compound@1.0.0")
COLUMNS = [ColumnSpec(name="id", type=FieldType.TEXT)]


@dataclass
class FakeDataConfig:
    max_filter_depth: int = 10
    max_predicates: int = 200
    max_feature_joins: int = 10
    max_page_limit: int = 1000


@dataclass
class FakeConfig:
    data: FakeDataConfig = field(default_factory=FakeDataConfig)


class FakeCatalogService:
    def __init__(self) -> None:
        self.resolved_with: tuple[Any, ...] | None = None

    async def resolve_table(self, schema, table_kind, feature_name=None) -> ResolvedTable:
        self.resolved_with = (schema, table_kind, feature_name)
        return ResolvedTable(schema_id=SCHEMA_ID, columns=COLUMNS)


class FakeQueryService:
    def __init__(self) -> None:
        self.received: tuple[QueryPlan, timedelta | None] | None = None

    async def _rows(self) -> AsyncIterator[Mapping[str, Any]]:
        yield {"id": "a"}

    def stream_records(self, plan: QueryPlan, timeout: timedelta | None = None):
        self.received = (plan, timeout)
        return self._rows()

    def stream_features(self, plan: QueryPlan, timeout: timedelta | None = None):
        self.received = (plan, timeout)
        return self._rows()


def _records_handler() -> tuple[ReadRecordsTableHandler, FakeCatalogService, FakeQueryService]:
    catalog, query = FakeCatalogService(), FakeQueryService()
    handler = ReadRecordsTableHandler(
        catalog_service=catalog, query_service=query, config=FakeConfig()
    )
    return handler, catalog, query


@pytest.mark.asyncio
async def test_records_handler_resolves_and_streams() -> None:
    handler, catalog, query = _records_handler()
    result = await handler.run(ReadRecordsTable(schema="compound@1.0.0"))

    assert catalog.resolved_with == ("compound@1.0.0", TableKind.RECORDS, None)
    assert result.columns == COLUMNS
    assert result.plan.schema_id == SCHEMA_ID
    assert result.plan.table_kind == TableKind.RECORDS
    assert [r async for r in result.rows] == [{"id": "a"}]


@pytest.mark.asyncio
async def test_records_handler_clamps_limit_to_config() -> None:
    handler, _, _ = _records_handler()
    handler.config.data.max_page_limit = 100
    result = await handler.run(ReadRecordsTable(schema="compound@1.0.0", limit=5000))
    assert result.plan.pagination.limit == 100


@pytest.mark.asyncio
async def test_records_handler_forwards_timeout_and_sort() -> None:
    handler, _, query = _records_handler()
    sort = [SortSpec(column="mw", direction=SortDirection.DESC)]
    result = await handler.run(
        ReadRecordsTable(schema="compound@1.0.0", sort=sort, timeout=timedelta(seconds=30))
    )
    plan, timeout = query.received
    assert plan is result.plan
    assert timeout == timedelta(seconds=30)
    assert plan.sort[0].column == "mw"


@pytest.mark.asyncio
async def test_feature_handler_resolves_feature_and_builds_feature_plan() -> None:
    catalog, query = FakeCatalogService(), FakeQueryService()
    handler = ReadFeatureTableHandler(
        catalog_service=catalog, query_service=query, config=FakeConfig()
    )
    result = await handler.run(ReadFeatureTable(schema="compound@1.0.0", feature="chem_features"))
    assert catalog.resolved_with == ("compound@1.0.0", TableKind.FEATURE, "chem_features")
    assert result.plan.table_kind == TableKind.FEATURE
    assert result.plan.feature_name == "chem_features"
