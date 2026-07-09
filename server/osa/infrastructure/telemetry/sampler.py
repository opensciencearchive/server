"""Periodic telemetry sampler for point-in-time gauges.

Some observability signals are *levels*, not events: outbox lag, per-group
pending/failed backlog, DB connection-pool occupancy, and live worker counts.
OpenTelemetry models these as **observable gauges** whose callbacks are invoked
*synchronously* by the metric reader at collection time. Our sources, however,
are async (a DB round-trip for delivery stats) and must not run inside a sync
callback.

:class:`TelemetrySampler` resolves that impedance mismatch: an async loop (owned
by :class:`~osa.infrastructure.event.worker.WorkerPool`) periodically calls
:meth:`refresh`, which samples every source and atomically swaps in a fresh
:class:`SamplerSnapshot`. The gauge callbacks are trivial sync closures that read
the latest snapshot and yield :class:`~opentelemetry.metrics.Observation` s. A
stale gauge (from a skipped refresh) is acceptable; a crashed sampler loop is
not — so :meth:`refresh` never propagates: on any error it warns and keeps the
previous snapshot.
"""

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from dishka import AsyncContainer
from opentelemetry.metrics import CallbackOptions, Meter, Observation
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.pool import QueuePool

from osa.domain.auth.model.identity import Identity, System
from osa.domain.shared.event import DeliveryStatus, WorkerStatus
from osa.domain.shared.port.event_repository import EventRepository
from osa.infrastructure.logging import get_logger
from osa.util.di.scope import Scope

if TYPE_CHECKING:
    from osa.infrastructure.event.worker import Worker

logger = get_logger(__name__)


@dataclass(frozen=True)
class PoolStats:
    """Point-in-time SQLAlchemy connection-pool occupancy."""

    checked_out: int
    size: int
    overflow: int


@dataclass(frozen=True)
class SamplerSnapshot:
    """Latest sampled values served to OTel gauge callbacks (sync) by the async refresh loop.

    Attributes:
        outbox_lag_seconds: Age of the oldest *eligible* pending delivery, or
            ``0.0`` when the queue is empty.
        pending_by_group: Pending delivery count per consumer group.
        failed_by_group: Failed delivery count per consumer group.
        pool: Connection-pool occupancy, or None when the engine has no
            meaningful pool (SQLite ``StaticPool``).
        workers_busy: Worker loops currently processing.
        workers_total: Total worker loops in the pool.
    """

    outbox_lag_seconds: float = 0.0
    pending_by_group: Mapping[str, int] = field(default_factory=dict)
    failed_by_group: Mapping[str, int] = field(default_factory=dict)
    pool: PoolStats | None = None
    workers_busy: int = 0
    workers_total: int = 0


class TelemetrySampler:
    """Bridges async periodic sampling to sync OTel observable-gauge callbacks.

    Owns the ``osa_outbox_*``, ``osa_db_pool_*``, and ``osa_workers_*`` gauges;
    each name is registered exactly once here. The gauges read from
    :attr:`_snapshot`, refreshed out-of-band by :meth:`refresh`.
    """

    def __init__(self, meter: Meter, engine: AsyncEngine) -> None:
        self._engine = engine
        self._snapshot = SamplerSnapshot()

        meter.create_observable_gauge(
            "osa_outbox_lag_seconds",
            callbacks=[self._observe_lag],
            unit="s",
            description="Age of the oldest eligible pending outbox delivery (claimable now).",
        )
        meter.create_observable_gauge(
            "osa_outbox_pending",
            callbacks=[self._observe_pending],
            description="Pending (unclaimed) deliveries per consumer group.",
        )
        meter.create_observable_gauge(
            "osa_outbox_failed",
            callbacks=[self._observe_failed],
            description="Failed deliveries per consumer group.",
        )
        meter.create_observable_gauge(
            "osa_db_pool_checked_out",
            callbacks=[self._observe_pool_checked_out],
            description="Connections currently checked out of the SQLAlchemy pool.",
        )
        meter.create_observable_gauge(
            "osa_db_pool_size",
            callbacks=[self._observe_pool_size],
            description="Configured base size of the connection pool.",
        )
        meter.create_observable_gauge(
            "osa_db_pool_overflow",
            callbacks=[self._observe_pool_overflow],
            description="Overflow connections open beyond the base pool size.",
        )
        meter.create_observable_gauge(
            "osa_workers_busy",
            callbacks=[self._observe_workers_busy],
            description="Worker loops currently processing.",
        )
        meter.create_observable_gauge(
            "osa_workers_total",
            callbacks=[self._observe_workers_total],
            description="Total worker loops in the pool.",
        )

    # ── Gauge callbacks (sync; read the latest snapshot) ──────────────────────

    def _observe_lag(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(self._snapshot.outbox_lag_seconds)

    def _observe_pending(self, options: CallbackOptions) -> Iterable[Observation]:
        for group, count in self._snapshot.pending_by_group.items():
            yield Observation(count, {"consumer_group": group})

    def _observe_failed(self, options: CallbackOptions) -> Iterable[Observation]:
        for group, count in self._snapshot.failed_by_group.items():
            yield Observation(count, {"consumer_group": group})

    def _observe_pool_checked_out(self, options: CallbackOptions) -> Iterable[Observation]:
        pool = self._snapshot.pool
        if pool is not None:
            yield Observation(pool.checked_out)

    def _observe_pool_size(self, options: CallbackOptions) -> Iterable[Observation]:
        pool = self._snapshot.pool
        if pool is not None:
            yield Observation(pool.size)

    def _observe_pool_overflow(self, options: CallbackOptions) -> Iterable[Observation]:
        pool = self._snapshot.pool
        if pool is not None:
            yield Observation(pool.overflow)

    def _observe_workers_busy(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(self._snapshot.workers_busy)

    def _observe_workers_total(self, options: CallbackOptions) -> Iterable[Observation]:
        yield Observation(self._snapshot.workers_total)

    # ── Async refresh (owned by the WorkerPool loop) ──────────────────────────

    async def refresh(self, container: AsyncContainer, workers: Sequence["Worker"]) -> None:
        """Sample every source and atomically swap in a fresh snapshot.

        Opens a UOW scope as the System identity (like the pool's other
        housekeeping loops), reads outbox delivery health, the DB pool, and the
        live worker states, then assembles a new :class:`SamplerSnapshot`.
        """
        try:
            async with container(scope=Scope.UOW, context={Identity: System()}) as scope:
                repo = await scope.get(EventRepository)
                stats = await repo.delivery_stats()

            oldest = stats.oldest_pending_created_at
            if oldest is not None:
                lag = max(0.0, (datetime.now(UTC) - oldest).total_seconds())
            else:
                lag = 0.0

            pending_by_group = {
                group: count
                for (group, status), count in stats.counts.items()
                if status is DeliveryStatus.PENDING
            }
            failed_by_group = {
                group: count
                for (group, status), count in stats.counts.items()
                if status is DeliveryStatus.FAILED
            }

            pool = self._engine.sync_engine.pool
            # SQLite StaticPool has no meaningful occupancy stats — skip it.
            pool_stats = (
                PoolStats(
                    checked_out=pool.checkedout(),
                    size=pool.size(),
                    overflow=pool.overflow(),
                )
                if isinstance(pool, QueuePool)
                else None
            )

            workers_busy = sum(1 for w in workers if w.state.status is WorkerStatus.PROCESSING)

            self._snapshot = SamplerSnapshot(
                outbox_lag_seconds=lag,
                pending_by_group=pending_by_group,
                failed_by_group=failed_by_group,
                pool=pool_stats,
                workers_busy=workers_busy,
                workers_total=len(workers),
            )
        except Exception as exc:
            # A stale gauge beats a crashed sampler: warn and keep the previous
            # snapshot so the loop survives and metrics merely go stale.
            logger.warn("Telemetry sampler refresh failed: {error}", error=str(exc))
