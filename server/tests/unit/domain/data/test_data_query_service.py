"""Unit tests for DataQueryService — filter-bounds validation + delegation.

Uses a fake DataReadStore (no DB), mirroring the discovery service tests.
"""

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import pytest

from osa.domain.data.model.filter import And, FilterOperator, Predicate
from osa.domain.data.model.query_plan import QueryPlan, TableKind
from osa.domain.data.service.data_query import DataQueryService
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import SchemaId

SCHEMA = SchemaId.parse("compound@1.0.0")


@dataclass
class FakeDataConfig:
    """The /data/ filter-bound knobs DataQueryService reads (config.data.*)."""

    max_filter_depth: int = 10
    max_predicates: int = 200
    max_feature_joins: int = 10


@dataclass
class FakeConfig:
    """Minimal stand-in exposing only ``config.data`` (avoids full env setup)."""

    data: FakeDataConfig = field(default_factory=FakeDataConfig)


class FakeReadStore:
    def __init__(self, rows: list[Mapping[str, Any]]) -> None:
        self._rows = rows
        self.received_plan: QueryPlan | None = None
        self.received_timeout: timedelta | None = None

    async def stream_rows(
        self, plan: QueryPlan, timeout: timedelta | None = None
    ) -> AsyncIterator[Mapping[str, Any]]:
        self.received_plan = plan
        self.received_timeout = timeout
        for row in self._rows:
            yield row

    async def get_record_by_id(self, id, version):  # pragma: no cover
        return None

    async def get_node_catalog(self):  # pragma: no cover
        ...

    async def get_schema_manifest(self, schema_id):  # pragma: no cover
        return None

    async def get_latest_schema_id(self, schema_short_id):  # pragma: no cover
        return None


def _service(rows=None) -> tuple[DataQueryService, FakeReadStore]:
    store = FakeReadStore(rows or [])
    return DataQueryService(read_store=store, config=FakeConfig()), store


def _pred(field: str = "metadata.mw") -> Predicate:
    return Predicate(field=field, op=FilterOperator.EQ, value=1)


async def _drain(gen: AsyncIterator[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [row async for row in gen]


@pytest.mark.asyncio
async def test_stream_records_delegates_to_store() -> None:
    service, store = _service([{"id": "a"}, {"id": "b"}])
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS)
    rows = await _drain(service.stream_records(plan))
    assert rows == [{"id": "a"}, {"id": "b"}]
    assert store.received_plan is plan


@pytest.mark.asyncio
async def test_stream_records_rejects_feature_plan() -> None:
    service, _ = _service()
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.FEATURE, feature_name="f")
    with pytest.raises(ValidationError):
        await _drain(service.stream_records(plan))


@pytest.mark.asyncio
async def test_filter_depth_bound_enforced() -> None:
    service, _ = _service()
    service.config.data.max_filter_depth = 1
    deep = And(operands=[_pred(), And(operands=[_pred(), _pred()])])
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS, filter=deep)
    with pytest.raises(ValidationError) as exc:
        await _drain(service.stream_records(plan))
    assert exc.value.code == "filter_depth_exceeded"


@pytest.mark.asyncio
async def test_filter_predicate_count_bound_enforced() -> None:
    service, _ = _service()
    service.config.data.max_predicates = 1
    plan = QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        filter=And(operands=[_pred(), _pred()]),
    )
    with pytest.raises(ValidationError) as exc:
        await _drain(service.stream_records(plan))
    assert exc.value.code == "filter_predicates_exceeded"


@pytest.mark.asyncio
async def test_no_filter_streams_cleanly() -> None:
    service, _ = _service([{"id": "x"}])
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS)
    assert await _drain(service.stream_records(plan)) == [{"id": "x"}]


@pytest.mark.asyncio
async def test_stream_records_passes_timeout_to_store() -> None:
    # The statement timeout is an execution budget owned by the caller (the
    # route's format registry); the service forwards it to the port so the
    # adapter — the only layer holding a SQL session — can apply it.
    service, store = _service([{"id": "a"}])
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS)
    await _drain(service.stream_records(plan, timeout=timedelta(seconds=30)))
    assert store.received_timeout == timedelta(seconds=30)


@pytest.mark.asyncio
async def test_stream_features_passes_timeout_to_store() -> None:
    service, store = _service([{"id": 1}])
    plan = QueryPlan(schema_id=SCHEMA, table_kind=TableKind.FEATURE, feature_name="f")
    await _drain(service.stream_features(plan, timeout=timedelta(minutes=30)))
    assert store.received_timeout == timedelta(minutes=30)
