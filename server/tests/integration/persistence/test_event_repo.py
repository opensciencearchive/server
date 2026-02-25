"""Integration tests for EventRepository against real PostgreSQL.

Tests PG-specific behavior: FOR UPDATE SKIP LOCKED, window functions
for fair round-robin, retry backoff with INTERVAL casts, partial indexes.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.domain.shared.event import Event, EventId
from osa.infrastructure.persistence.repository.event import SQLAlchemyEventRepository
from osa.infrastructure.persistence.tables import events_table


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

        await repo.save(event)
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

    async def test_save_with_routing_key(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        event = PingEvent(id=EventId(uuid4()), data="routed")

        await repo.save(event, routing_key="vector")
        await pg_session.commit()

        got = await repo.get(event.id)
        assert got is not None


@pytest.mark.asyncio
class TestEventRepoClaim:
    async def test_claim_returns_pending_events(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        e1 = PingEvent(id=EventId(uuid4()), data="first")
        e2 = PingEvent(id=EventId(uuid4()), data="second")
        await repo.save(e1)
        await repo.save(e2)
        await pg_session.commit()

        result = await repo.claim(event_types=["PingEvent"], limit=10)
        await pg_session.commit()

        assert len(result.events) == 2
        data_values = {e.data for e in result.events}
        assert data_values == {"first", "second"}

    async def test_claim_respects_limit(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        for i in range(5):
            await repo.save(PingEvent(id=EventId(uuid4()), data=f"e{i}"))
        await pg_session.commit()

        result = await repo.claim(event_types=["PingEvent"], limit=2)
        assert len(result.events) == 2

    async def test_claim_filters_by_event_type(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        await repo.save(PingEvent(id=EventId(uuid4()), data="ping"))
        await repo.save(PongEvent(id=EventId(uuid4()), data="pong"))
        await pg_session.commit()

        result = await repo.claim(event_types=["PongEvent"], limit=10)
        await pg_session.commit()

        assert len(result.events) == 1
        assert isinstance(result.events[0], PongEvent)
        assert result.events[0].data == "pong"

    async def test_claim_filters_by_routing_key(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        await repo.save(PingEvent(id=EventId(uuid4()), data="routed"), routing_key="vector")
        await repo.save(PingEvent(id=EventId(uuid4()), data="unrouted"))
        await pg_session.commit()

        result = await repo.claim(event_types=["PingEvent"], limit=10, routing_key="vector")
        await pg_session.commit()

        assert len(result.events) == 1
        assert result.events[0].data == "routed"

    async def test_claim_concurrent_sessions_see_disjoint_events(self, pg_engine: AsyncEngine):
        """Two concurrent sessions using FOR UPDATE SKIP LOCKED get disjoint sets."""
        factory = async_sessionmaker(pg_engine, expire_on_commit=False)

        # Seed events in a dedicated session
        async with factory() as seed_session:
            repo = SQLAlchemyEventRepository(seed_session)
            ids = []
            for i in range(6):
                eid = EventId(uuid4())
                ids.append(eid)
                await repo.save(PingEvent(id=eid, data=f"evt-{i}"))
            await seed_session.commit()

        # Two sessions claim concurrently
        async def claim_in_session(limit: int) -> list[EventId]:
            async with factory() as session:
                repo = SQLAlchemyEventRepository(session)
                result = await repo.claim(event_types=["PingEvent"], limit=limit)
                await session.commit()
                return [e.id for e in result.events]

        results = await asyncio.gather(
            claim_in_session(3),
            claim_in_session(3),
        )

        set_a = set(results[0])
        set_b = set(results[1])

        # No overlap — FOR UPDATE SKIP LOCKED ensures disjoint
        assert set_a.isdisjoint(set_b)
        # All 6 events claimed between the two
        assert len(set_a) + len(set_b) == 6


@pytest.mark.asyncio
class TestEventRepoFindPending:
    async def test_find_pending_fair_round_robin(self, pg_session: AsyncSession):
        """Fair mode interleaves event types."""
        repo = SQLAlchemyEventRepository(pg_session)

        # 3 PingEvents, 3 PongEvents
        for i in range(3):
            await repo.save(PingEvent(id=EventId(uuid4()), data=f"ping-{i}"))
            await repo.save(PongEvent(id=EventId(uuid4()), data=f"pong-{i}"))
        await pg_session.commit()

        result = await repo.find_pending(limit=4, fair=True)
        assert len(result) == 4

        # With fair=True, we should get a mix of both types
        types = [type(e).__name__ for e in result]
        assert "PingEvent" in types
        assert "PongEvent" in types

    async def test_find_pending_strict_fifo(self, pg_session: AsyncSession):
        """Strict FIFO returns events ordered by created_at regardless of type."""
        repo = SQLAlchemyEventRepository(pg_session)

        await repo.save(PingEvent(id=EventId(uuid4()), data="ping-0"))
        await repo.save(PongEvent(id=EventId(uuid4()), data="pong-0"))
        await pg_session.commit()

        result = await repo.find_pending(limit=10, fair=False)
        assert len(result) == 2
        # Strict FIFO: ordered by created_at
        assert result[0].data == "ping-0"
        assert result[1].data == "pong-0"


@pytest.mark.asyncio
class TestEventRepoRetry:
    async def test_mark_failed_with_retry_resets_to_pending(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        event = PingEvent(id=EventId(uuid4()), data="retry-me")
        await repo.save(event)
        await pg_session.commit()

        # Claim it
        await repo.claim(event_types=["PingEvent"], limit=1)
        await pg_session.commit()

        # Fail with retries remaining
        await repo.mark_failed_with_retry(event.id, "transient error", max_retries=3)
        await pg_session.commit()

        # Should be pending again with retry_count=1
        row = await pg_session.execute(
            events_table.select().where(events_table.c.id == str(event.id))
        )
        data = row.mappings().first()
        assert data is not None
        assert data["delivery_status"] == "pending"
        assert data["retry_count"] == 1

    async def test_mark_failed_after_max_retries_sets_failed(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        event = PingEvent(id=EventId(uuid4()), data="will-fail")
        await repo.save(event)
        await pg_session.commit()

        # Exhaust retries
        for _ in range(3):
            await repo.claim(event_types=["PingEvent"], limit=1)
            await pg_session.commit()
            await repo.mark_failed_with_retry(event.id, "error", max_retries=3)
            await pg_session.commit()

        row = await pg_session.execute(
            events_table.select().where(events_table.c.id == str(event.id))
        )
        data = row.mappings().first()
        assert data is not None
        assert data["delivery_status"] == "failed"
        assert data["retry_count"] == 3


@pytest.mark.asyncio
class TestEventRepoStaleClaims:
    async def test_reset_stale_claims(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        event = PingEvent(id=EventId(uuid4()), data="stale")
        await repo.save(event)
        await pg_session.commit()

        # Claim it, then backdate claimed_at to simulate staleness
        await repo.claim(event_types=["PingEvent"], limit=1)
        await pg_session.commit()

        stale_time = datetime.now(UTC) - timedelta(seconds=600)
        await pg_session.execute(
            update(events_table)
            .where(events_table.c.id == str(event.id))
            .values(claimed_at=stale_time)
        )
        await pg_session.commit()

        reset_count = await repo.reset_stale_claims(timeout_seconds=300)
        await pg_session.commit()

        assert reset_count == 1

        # Event should be pending again
        row = await pg_session.execute(
            events_table.select().where(events_table.c.id == str(event.id))
        )
        data = row.mappings().first()
        assert data is not None
        assert data["delivery_status"] == "pending"


@pytest.mark.asyncio
class TestEventRepoFindLatestByType:
    async def test_find_latest_by_type(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)

        e1 = PingEvent(id=EventId(uuid4()), data="older")
        e2 = PingEvent(id=EventId(uuid4()), data="newer")
        e3 = PongEvent(id=EventId(uuid4()), data="different-type")

        await repo.save(e1)
        await repo.save(e2)
        await repo.save(e3)
        await pg_session.commit()

        latest = await repo.find_latest_by_type(PingEvent)
        assert latest is not None
        assert isinstance(latest, PingEvent)
        assert latest.data == "newer"

    async def test_find_latest_by_type_none_when_empty(self, pg_session: AsyncSession):
        repo = SQLAlchemyEventRepository(pg_session)
        latest = await repo.find_latest_by_type(PingEvent)
        assert latest is None
