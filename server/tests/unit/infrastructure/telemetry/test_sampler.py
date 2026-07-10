"""Unit tests for the periodic :class:`TelemetrySampler`.

The sampler bridges async periodic sampling (outbox delivery health, DB pool
stats, worker states) to *sync* OpenTelemetry observable-gauge callbacks. These
tests drive it against an in-memory meter provider and assert the exported gauge
points for name, labels, and value — the red-first spec for P6.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    Gauge,
    InMemoryMetricReader,
    Metric,
)
from sqlalchemy.ext.asyncio import create_async_engine

from osa.domain.shared.event import DeliveryStats, DeliveryStatus, WorkerStatus
from osa.domain.shared.port.event_repository import EventRepository
from osa.infrastructure.telemetry.sampler import (
    PoolStats,
    SamplerSnapshot,
    TelemetrySampler,
)


@pytest.fixture
def reader() -> InMemoryMetricReader:
    return InMemoryMetricReader()


@pytest.fixture
def meter(reader: InMemoryMetricReader):
    provider = MeterProvider(metric_readers=[reader])
    return provider.get_meter("test")


def _metrics(reader: InMemoryMetricReader) -> Iterator[Metric]:
    data = reader.get_metrics_data()
    if data is None:
        return
    for rm in data.resource_metrics:
        for sm in rm.scope_metrics:
            yield from sm.metrics


def _points(reader: InMemoryMetricReader, name: str) -> list[tuple[dict[str, object], object]]:
    for metric in _metrics(reader):
        if metric.name == name:
            assert isinstance(metric.data, Gauge)
            return [(dict(p.attributes), p.value) for p in metric.data.data_points]
    return []


@dataclass
class _FakeRepo:
    """EventRepository stub exposing only ``delivery_stats``."""

    stats: DeliveryStats

    async def delivery_stats(self) -> DeliveryStats:
        return self.stats


def _make_container(repo: object) -> MagicMock:
    """Mock DI container whose UOW scope resolves ``EventRepository`` to ``repo``."""

    async def get_dependency(cls):
        if cls is EventRepository:
            return repo
        raise AssertionError(f"unexpected dependency requested: {cls!r}")

    scope = AsyncMock()
    scope.get = AsyncMock(side_effect=get_dependency)

    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=scope)
    context.__aexit__ = AsyncMock(return_value=None)

    container = MagicMock()
    container.return_value = context
    return container


def _fake_worker(status: WorkerStatus) -> object:
    """Minimal object exposing ``.state.status`` like a real Worker."""
    return SimpleNamespace(state=SimpleNamespace(status=status))


def _pg_engine():
    """A QueuePool-backed engine (never connects) so pool gauges are populated."""
    return create_async_engine("postgresql+asyncpg://u:p@localhost/db")


def _sqlite_engine():
    """A StaticPool-backed engine so pool stats are absent."""
    return create_async_engine("sqlite+aiosqlite:///:memory:")


# ── Fresh sampler (no refresh) ────────────────────────────────────────────────


def test_fresh_sampler_emits_zeroes_and_no_pool(reader, meter):
    TelemetrySampler(meter, _pg_engine())

    assert _points(reader, "osa_outbox_lag_seconds") == [({}, 0.0)]
    assert _points(reader, "osa_outbox_pending") == []
    assert _points(reader, "osa_outbox_failed") == []
    assert _points(reader, "osa_db_pool_checked_out") == []
    assert _points(reader, "osa_db_pool_size") == []
    assert _points(reader, "osa_db_pool_overflow") == []
    assert _points(reader, "osa_workers_busy") == [({}, 0)]
    assert _points(reader, "osa_workers_total") == [({}, 0)]


# ── After a good refresh ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_populates_all_gauges(reader, meter):
    sampler = TelemetrySampler(meter, _pg_engine())

    oldest = datetime.now(UTC) - timedelta(seconds=12)
    stats = DeliveryStats(
        counts={
            ("ProcessBatch", DeliveryStatus.PENDING): 3,
            ("ProcessSubmission", DeliveryStatus.PENDING): 1,
            ("ProcessBatch", DeliveryStatus.FAILED): 2,
            ("ProcessBatch", DeliveryStatus.DELIVERED): 99,
        },
        oldest_pending_created_at=oldest,
    )
    container = _make_container(_FakeRepo(stats))
    workers = [
        _fake_worker(WorkerStatus.PROCESSING),
        _fake_worker(WorkerStatus.PROCESSING),
        _fake_worker(WorkerStatus.IDLE),
    ]

    await sampler.refresh(container, workers)

    (lag_attrs, lag_val) = _points(reader, "osa_outbox_lag_seconds")[0]
    assert lag_attrs == {}
    assert lag_val == pytest.approx(12.0, abs=2.0)

    pending = {a["consumer_group"]: v for a, v in _points(reader, "osa_outbox_pending")}
    assert pending == {"ProcessBatch": 3, "ProcessSubmission": 1}

    failed = {a["consumer_group"]: v for a, v in _points(reader, "osa_outbox_failed")}
    assert failed == {"ProcessBatch": 2}

    assert _points(reader, "osa_workers_busy") == [({}, 2)]
    assert _points(reader, "osa_workers_total") == [({}, 3)]

    # QueuePool → pool gauges present (single unlabelled point each).
    assert len(_points(reader, "osa_db_pool_size")) == 1
    assert len(_points(reader, "osa_db_pool_checked_out")) == 1
    assert len(_points(reader, "osa_db_pool_overflow")) == 1


@pytest.mark.asyncio
async def test_refresh_no_pending_reports_zero_lag(reader, meter):
    sampler = TelemetrySampler(meter, _pg_engine())
    stats = DeliveryStats(counts={}, oldest_pending_created_at=None)
    container = _make_container(_FakeRepo(stats))

    await sampler.refresh(container, [])

    assert _points(reader, "osa_outbox_lag_seconds") == [({}, 0.0)]


# ── SQLite StaticPool → no pool gauges ────────────────────────────────────────


@pytest.mark.asyncio
async def test_static_pool_yields_no_pool_gauges(reader, meter):
    sampler = TelemetrySampler(meter, _sqlite_engine())
    stats = DeliveryStats(counts={}, oldest_pending_created_at=None)
    container = _make_container(_FakeRepo(stats))

    await sampler.refresh(container, [])

    assert sampler._snapshot.pool is None
    assert _points(reader, "osa_db_pool_checked_out") == []
    assert _points(reader, "osa_db_pool_size") == []
    assert _points(reader, "osa_db_pool_overflow") == []


# ── Refresh error keeps the last good snapshot ────────────────────────────────


@pytest.mark.asyncio
async def test_refresh_error_keeps_previous_snapshot(meter, monkeypatch):
    import osa.infrastructure.telemetry.sampler as sampler_module

    fake_logger = MagicMock()
    monkeypatch.setattr(sampler_module, "logger", fake_logger)

    sampler = TelemetrySampler(meter, _pg_engine())

    good = SamplerSnapshot(
        outbox_lag_seconds=5.0,
        pending_by_group={"ProcessBatch": 7},
        failed_by_group={},
        pool=PoolStats(checked_out=1, size=5, overflow=0),
        workers_busy=1,
        workers_total=2,
    )
    sampler._snapshot = good

    class _BoomRepo:
        async def delivery_stats(self) -> DeliveryStats:
            raise RuntimeError("db down")

    container = _make_container(_BoomRepo())

    await sampler.refresh(container, [])

    assert sampler._snapshot is good  # unchanged
    assert fake_logger.warn.call_count == 1


# ── WorkerPool wiring ─────────────────────────────────────────────────────────


class _StubSampler:
    """Records refresh calls; stands in for a real TelemetrySampler."""

    def __init__(self) -> None:
        self.refresh_calls = 0

    async def refresh(self, container, workers) -> None:  # noqa: ANN001
        self.refresh_calls += 1


@pytest.mark.asyncio
async def test_worker_pool_without_sampler_creates_no_task():
    from tests.unit.infrastructure.event.test_worker_pool import (
        DummyHandler,
        make_mock_container,
    )

    from osa.infrastructure.event.worker import WorkerPool

    pool = WorkerPool(container=make_mock_container(), stale_claim_interval=0)
    pool.register(DummyHandler)

    await pool.start()
    try:
        assert pool._telemetry_sampler_task is None
    finally:
        await pool.stop()


@pytest.mark.asyncio
async def test_worker_pool_with_sampler_runs_and_cancels():
    from tests.unit.infrastructure.event.test_worker_pool import (
        DummyHandler,
        make_mock_container,
    )

    from osa.infrastructure.event.worker import WorkerPool

    sampler = _StubSampler()
    pool = WorkerPool(
        container=make_mock_container(),
        stale_claim_interval=0,
        sampler=sampler,  # type: ignore[arg-type]
        sampler_interval=0.02,
    )
    pool.register(DummyHandler)

    await pool.start()
    try:
        assert pool._telemetry_sampler_task is not None
        # Let the loop tick at least once.
        import asyncio

        await asyncio.sleep(0.05)
        assert sampler.refresh_calls >= 1
    finally:
        await pool.stop()

    assert pool._telemetry_sampler_task.done()
