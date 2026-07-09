"""Unit tests for the outbox delivery-health aggregation query.

Exercises ``SQLAlchemyEventRepository.delivery_stats`` against an in-memory
SQLite engine (events + deliveries tables only):
- an empty database yields no counts and no pending lag,
- counts are keyed exactly by ``(consumer_group, DeliveryStatus)``,
- only *eligible* pending deliveries (``deliver_after`` NULL or in the past)
  contribute to ``oldest_pending_created_at`` — scheduled-for-later rows are
  excluded,
- the returned timestamp is timezone-aware (SQLite normalization).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import StaticPool

from osa.domain.shared.event import DeliveryStatus
from osa.infrastructure.persistence.repository.event import SQLAlchemyEventRepository
from osa.infrastructure.persistence.tables import deliveries_table, events_table, metadata


@pytest.fixture
async def sqlite_session() -> AsyncIterator[AsyncSession]:
    """In-memory SQLite session with only the events/deliveries tables created."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(
            lambda sync_conn: metadata.create_all(
                sync_conn, tables=[events_table, deliveries_table]
            )
        )
    async with AsyncSession(engine, expire_on_commit=False) as session:
        yield session
    await engine.dispose()


async def _seed(
    session: AsyncSession,
    *,
    consumer_group: str,
    status: DeliveryStatus,
    created_at: datetime,
    deliver_after: datetime | None = None,
) -> None:
    """Insert one event and its delivery row with fully controlled fields."""
    event_id = str(uuid4())
    await session.execute(
        insert(events_table).values(
            id=event_id,
            event_type="TraceEvent",
            payload={"id": event_id, "data": "x"},
            created_at=created_at,
            trace_context=None,
        )
    )
    await session.execute(
        insert(deliveries_table).values(
            id=str(uuid4()),
            event_id=event_id,
            consumer_group=consumer_group,
            status=status.value,
            retry_count=0,
            deliver_after=deliver_after,
            updated_at=created_at,
        )
    )


@pytest.mark.asyncio
class TestDeliveryStats:
    async def test_empty_db_yields_no_counts_and_no_lag(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)

        stats = await repo.delivery_stats()

        assert stats.counts == {}
        assert stats.oldest_pending_created_at is None

    async def test_counts_keyed_by_group_and_status(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)
        now = datetime.now(UTC)

        await _seed(
            sqlite_session, consumer_group="g1", status=DeliveryStatus.PENDING, created_at=now
        )
        await _seed(
            sqlite_session, consumer_group="g1", status=DeliveryStatus.PENDING, created_at=now
        )
        await _seed(
            sqlite_session, consumer_group="g1", status=DeliveryStatus.DELIVERED, created_at=now
        )
        await _seed(
            sqlite_session, consumer_group="g2", status=DeliveryStatus.FAILED, created_at=now
        )
        await sqlite_session.commit()

        stats = await repo.delivery_stats()

        assert stats.counts == {
            ("g1", DeliveryStatus.PENDING): 2,
            ("g1", DeliveryStatus.DELIVERED): 1,
            ("g2", DeliveryStatus.FAILED): 1,
        }

    async def test_future_deliver_after_excluded_from_lag(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)
        now = datetime.now(UTC)

        # Earliest event, but scheduled for the future -> ineligible, must NOT count.
        await _seed(
            sqlite_session,
            consumer_group="g1",
            status=DeliveryStatus.PENDING,
            created_at=now - timedelta(hours=2),
            deliver_after=now + timedelta(hours=1),
        )
        # Eligible: past deliver_after.
        await _seed(
            sqlite_session,
            consumer_group="g1",
            status=DeliveryStatus.PENDING,
            created_at=now - timedelta(minutes=30),
            deliver_after=now - timedelta(minutes=5),
        )
        # Eligible: NULL deliver_after, oldest eligible event.
        await _seed(
            sqlite_session,
            consumer_group="g2",
            status=DeliveryStatus.PENDING,
            created_at=now - timedelta(minutes=45),
            deliver_after=None,
        )
        await sqlite_session.commit()

        stats = await repo.delivery_stats()

        assert stats.oldest_pending_created_at is not None
        # Oldest eligible is the NULL-deliver_after row at now-45m,
        # NOT the future-scheduled now-2h row.
        expected = now - timedelta(minutes=45)
        delta = abs((stats.oldest_pending_created_at - expected).total_seconds())
        assert delta < 1.0

    async def test_returned_timestamp_is_timezone_aware(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)
        now = datetime.now(UTC)

        await _seed(
            sqlite_session,
            consumer_group="g1",
            status=DeliveryStatus.PENDING,
            created_at=now - timedelta(minutes=10),
        )
        await sqlite_session.commit()

        stats = await repo.delivery_stats()

        assert stats.oldest_pending_created_at is not None
        assert stats.oldest_pending_created_at.tzinfo is not None

    async def test_non_pending_never_contributes_to_lag(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)
        now = datetime.now(UTC)

        # Only non-pending rows -> no eligible pending -> no lag.
        await _seed(
            sqlite_session,
            consumer_group="g1",
            status=DeliveryStatus.DELIVERED,
            created_at=now - timedelta(hours=3),
        )
        await _seed(
            sqlite_session,
            consumer_group="g1",
            status=DeliveryStatus.FAILED,
            created_at=now - timedelta(hours=4),
        )
        await sqlite_session.commit()

        stats = await repo.delivery_stats()

        assert stats.oldest_pending_created_at is None
