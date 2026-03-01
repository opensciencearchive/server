"""Integration tests for EventRepository against real PostgreSQL.

Tests PG-specific behavior: FOR UPDATE SKIP LOCKED on deliveries,
consumer-group delivery model, retry backoff, stale claim detection.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.domain.shared.event import Event, EventId
from osa.infrastructure.persistence.repository.event import SQLAlchemyEventRepository
from osa.infrastructure.persistence.tables import deliveries_table

CONSUMER_GROUP = "test-group"


class PingEvent(Event):
    """Test event type A."""

    id: EventId
    data: str


class PongEvent(Event):
    """Test event type B."""

    id: EventId
    data: str


@pytest.mark.asyncio
class TestEventRepoSaveAndGet:
    async def test_save_and_get_round_trip(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        event = PingEvent(id=EventId(uuid4()), data="hello")

        await repo.save_with_deliveries(event, {CONSUMER_GROUP})
        await pg_session.commit()

        got = await repo.get(event.id)
        assert got is not None
        assert isinstance(got, PingEvent)
        assert got.id == event.id
        assert got.data == "hello"

    async def test_get_nonexistent_returns_none(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        got = await repo.get(EventId(uuid4()))
        assert got is None


@pytest.mark.asyncio
class TestEventRepoClaimDelivery:
    async def test_claim_returns_pending_deliveries(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        e1 = PingEvent(id=EventId(uuid4()), data="first")
        e2 = PingEvent(id=EventId(uuid4()), data="second")
        await repo.save_with_deliveries(e1, {CONSUMER_GROUP})
        await repo.save_with_deliveries(e2, {CONSUMER_GROUP})
        await pg_session.commit()

        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["PingEvent"], limit=10
        )
        await pg_session.commit()

        assert len(result) == 2
        data_values = {d.event.data for d in result.deliveries}
        assert data_values == {"first", "second"}

    async def test_claim_respects_limit(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        for i in range(5):
            await repo.save_with_deliveries(
                PingEvent(id=EventId(uuid4()), data=f"e{i}"), {CONSUMER_GROUP}
            )
        await pg_session.commit()

        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["PingEvent"], limit=2
        )
        assert len(result) == 2

    async def test_claim_filters_by_event_type(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        await repo.save_with_deliveries(
            PingEvent(id=EventId(uuid4()), data="ping"), {CONSUMER_GROUP}
        )
        await repo.save_with_deliveries(
            PongEvent(id=EventId(uuid4()), data="pong"), {CONSUMER_GROUP}
        )
        await pg_session.commit()

        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["PongEvent"], limit=10
        )
        await pg_session.commit()

        assert len(result) == 1
        assert isinstance(result.deliveries[0].event, PongEvent)
        assert result.deliveries[0].event.data == "pong"

    async def test_claim_concurrent_sessions_see_disjoint_deliveries(self, pg_engine: AsyncEngine):
        """Two concurrent sessions using FOR UPDATE SKIP LOCKED get disjoint sets."""
        factory = async_sessionmaker(pg_engine, expire_on_commit=False)

        # Seed events in a dedicated session
        async with factory() as seed_session:
            repo = SQLAlchemyEventRepository(seed_session)
            for i in range(6):
                await repo.save_with_deliveries(
                    PingEvent(id=EventId(uuid4()), data=f"evt-{i}"), {CONSUMER_GROUP}
                )
            await seed_session.commit()

        # Two sessions claim concurrently
        async def claim_in_session(limit: int) -> list[str]:
            async with factory() as session:
                repo = SQLAlchemyEventRepository(session)
                result = await repo.claim_delivery(
                    consumer_group=CONSUMER_GROUP,
                    event_types=["PingEvent"],
                    limit=limit,
                )
                await session.commit()
                return [d.id for d in result.deliveries]

        results = await asyncio.gather(
            claim_in_session(3),
            claim_in_session(3),
        )

        set_a = set(results[0])
        set_b = set(results[1])

        # No overlap — FOR UPDATE SKIP LOCKED ensures disjoint
        assert set_a.isdisjoint(set_b)
        # All 6 deliveries claimed between the two
        assert len(set_a) + len(set_b) == 6


@pytest.mark.asyncio
class TestEventRepoRetry:
    async def test_mark_failed_with_retry_resets_to_pending(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        event = PingEvent(id=EventId(uuid4()), data="retry-me")
        await repo.save_with_deliveries(event, {CONSUMER_GROUP})
        await pg_session.commit()

        # Claim it
        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["PingEvent"], limit=1
        )
        delivery_id = result.deliveries[0].id
        await pg_session.commit()

        # Fail with retries remaining
        await repo.mark_failed_with_retry(delivery_id, "transient error", max_retries=3)
        await pg_session.commit()

        # Should be pending again with retry_count=1
        row = await pg_session.execute(
            deliveries_table.select().where(deliveries_table.c.id == delivery_id)
        )
        data = row.mappings().first()
        assert data is not None
        assert data["status"] == "pending"
        assert data["retry_count"] == 1

    async def test_mark_failed_after_max_retries_sets_failed(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        event = PingEvent(id=EventId(uuid4()), data="will-fail")
        await repo.save_with_deliveries(event, {CONSUMER_GROUP})
        await pg_session.commit()

        delivery_id: str | None = None

        # Exhaust retries — backdate updated_at after each failure so the
        # backoff window is satisfied and the delivery becomes claimable again.
        past = datetime.now(UTC) - timedelta(seconds=60)
        for _ in range(3):
            if delivery_id is not None:
                await pg_session.execute(
                    update(deliveries_table)
                    .where(deliveries_table.c.id == delivery_id)
                    .values(updated_at=past)
                )
                await pg_session.commit()

            result = await repo.claim_delivery(
                consumer_group=CONSUMER_GROUP, event_types=["PingEvent"], limit=1
            )
            delivery_id = result.deliveries[0].id
            await pg_session.commit()
            await repo.mark_failed_with_retry(delivery_id, "error", max_retries=3)
            await pg_session.commit()

        row = await pg_session.execute(
            deliveries_table.select().where(deliveries_table.c.id == delivery_id)
        )
        data = row.mappings().first()
        assert data is not None
        assert data["status"] == "failed"
        assert data["retry_count"] == 3


@pytest.mark.asyncio
class TestEventRepoStaleDeliveries:
    async def test_reset_stale_deliveries(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        event = PingEvent(id=EventId(uuid4()), data="stale")
        await repo.save_with_deliveries(event, {CONSUMER_GROUP})
        await pg_session.commit()

        # Claim it, then backdate claimed_at to simulate staleness
        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["PingEvent"], limit=1
        )
        delivery_id = result.deliveries[0].id
        await pg_session.commit()

        stale_time = datetime.now(UTC) - timedelta(seconds=600)
        await pg_session.execute(
            update(deliveries_table)
            .where(deliveries_table.c.id == delivery_id)
            .values(claimed_at=stale_time)
        )
        await pg_session.commit()

        reset_count = await repo.reset_stale_deliveries(timeout_seconds=300)
        await pg_session.commit()

        assert reset_count == 1

        # Delivery should be pending again
        row = await pg_session.execute(
            deliveries_table.select().where(deliveries_table.c.id == delivery_id)
        )
        data = row.mappings().first()
        assert data is not None
        assert data["status"] == "pending"


@pytest.mark.asyncio
class TestEventRepoFindLatestByType:
    async def test_find_latest_by_type(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        e1 = PingEvent(id=EventId(uuid4()), data="older")
        e2 = PingEvent(id=EventId(uuid4()), data="newer")
        e3 = PongEvent(id=EventId(uuid4()), data="different-type")

        await repo.save_with_deliveries(e1, {CONSUMER_GROUP})
        await repo.save_with_deliveries(e2, {CONSUMER_GROUP})
        await repo.save_with_deliveries(e3, {CONSUMER_GROUP})
        await pg_session.commit()

        latest = await repo.find_latest_by_type(PingEvent)
        assert latest is not None
        assert isinstance(latest, PingEvent)
        assert latest.data == "newer"

    async def test_find_latest_by_type_none_when_empty(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        latest = await repo.find_latest_by_type(PingEvent)
        assert latest is None
