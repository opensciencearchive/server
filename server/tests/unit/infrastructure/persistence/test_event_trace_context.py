"""Unit tests for W3C trace-context capture through the outbox.

Exercises SQLAlchemyEventRepository against an in-memory SQLite engine:
- the traceparent of the appending operation is captured on the events row
  and rehydrated onto the claimed Delivery (round-trip),
- non-instrumented contexts (no active span) leave the column NULL,
- the DeliveryStatus vocabulary round-trips its known string values.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.pool import StaticPool

from osa.domain.ingest.event.events import NextBatchRequested  # noqa: F401 — registers the type
from osa.domain.shared.event import Delivery, DeliveryStatus, Event, EventId
from osa.infrastructure.persistence.repository.event import SQLAlchemyEventRepository
from osa.infrastructure.persistence.tables import deliveries_table, events_table, metadata

CONSUMER_GROUP = "test-group"
_TRACE_ID = 0x1234567890ABCDEF1234567890ABCDEF
_SPAN_ID = 0x1A2B3C4D5E6F7788


class TraceEvent(Event):
    """Test event type for trace-context round-trips."""

    id: EventId
    data: str


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


def _sampled_span_token() -> object:
    """Attach a deterministic, sampled current span (no tracer SDK needed)."""
    sc = SpanContext(
        trace_id=_TRACE_ID,
        span_id=_SPAN_ID,
        is_remote=False,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return otel_context.attach(otel_trace.set_span_in_context(NonRecordingSpan(sc)))


@pytest.mark.asyncio
class TestTraceContextRoundTrip:
    async def test_traceparent_captured_and_rehydrated(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)
        event = TraceEvent(id=EventId(uuid4()), data="traced")

        token = _sampled_span_token()
        try:
            await repo.save_with_deliveries(event, {CONSUMER_GROUP})
        finally:
            otel_context.detach(token)
        await sqlite_session.commit()

        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["TraceEvent"], limit=1
        )
        await sqlite_session.commit()

        assert len(result) == 1
        delivery = result.deliveries[0]
        assert delivery.trace_context is not None
        # W3C traceparent: 00-<32 hex trace id>-<16 hex span id>-<flags>
        parts = delivery.trace_context.split("-")
        assert len(parts) == 4
        assert parts[0] == "00"
        assert parts[1] == f"{_TRACE_ID:032x}"
        assert parts[3] == "01"  # sampled

    async def test_no_span_leaves_trace_context_null(self, sqlite_session: AsyncSession):
        repo = SQLAlchemyEventRepository(sqlite_session)
        event = TraceEvent(id=EventId(uuid4()), data="untraced")

        # No active recording span (CLI/cron path).
        await repo.save_with_deliveries(event, {CONSUMER_GROUP})
        await sqlite_session.commit()

        result = await repo.claim_delivery(
            consumer_group=CONSUMER_GROUP, event_types=["TraceEvent"], limit=1
        )
        await sqlite_session.commit()

        assert len(result) == 1
        assert result.deliveries[0].trace_context is None


class TestDeliveryStatusEnum:
    def test_known_values_round_trip(self) -> None:
        expected = {
            "pending": DeliveryStatus.PENDING,
            "claimed": DeliveryStatus.CLAIMED,
            "delivered": DeliveryStatus.DELIVERED,
            "failed": DeliveryStatus.FAILED,
            "skipped": DeliveryStatus.SKIPPED,
        }
        for value, member in expected.items():
            assert member.value == value
            assert DeliveryStatus(value) is member

    def test_default_delivery_trace_context_is_none(self) -> None:
        event = TraceEvent(id=EventId(uuid4()), data="x")
        assert Delivery(id="d-1", event=event).trace_context is None


class TestLegacyPayloadDeserialization:
    """Mixed-version event rows must degrade gracefully, not 500 the changefeed.

    Pre-#160 ``NextBatchRequested`` rows have no ``batch_index``; deserializing
    one must warn-and-skip (return None) rather than raise — a deployment note in
    PR #161. Exercises the private ``_deserialize`` directly (no DB needed).
    """

    def test_legacy_next_batch_requested_payload_skipped_not_raised(self) -> None:
        repo = SQLAlchemyEventRepository(MagicMock())
        legacy_payload = {
            "id": str(uuid4()),
            "ingest_run_id": "run-1",
            "convention_id": "test-conv",
            "batch_size": 100,
            "created_at": "2026-01-01T00:00:00+00:00",
        }

        assert repo._deserialize("NextBatchRequested", legacy_payload) is None
