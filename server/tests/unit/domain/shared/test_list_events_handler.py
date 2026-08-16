"""ListEvents query handler (arch-survey 2026-08-16 F1).

The /events route previously injected EventLog directly — full event payloads
to anonymous callers with no gate anywhere. The handler now owns the changefeed
read (limit+1 look-ahead, cursor, payload shaping) behind an explicit,
boot-validated gate. Public is the deliberate choice: the changefeed is the
federation surface (CLAUDE.md API §9) — the gate documents that decision.
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.shared.authorization.gate import Public
from osa.domain.shared.event import Event, EventId
from osa.domain.shared.event_log import ListEvents, ListEventsHandler


class SomethingHappened(Event):
    detail: str


def _event(detail: str) -> SomethingHappened:
    return SomethingHappened(
        id=EventId(uuid4()), created_at=datetime(2026, 1, 1, tzinfo=UTC), detail=detail
    )


def _handler(events: list[Event]) -> ListEventsHandler:
    log = AsyncMock()
    log.list_events.return_value = events
    return ListEventsHandler(event_log=log)


class TestGate:
    def test_gate_is_explicitly_public(self):
        assert isinstance(ListEventsHandler.__auth__, Public)


@pytest.mark.asyncio
class TestChangefeedPage:
    async def test_look_ahead_sets_has_more_and_trims(self):
        events = [_event(f"e{i}") for i in range(3)]
        handler = _handler(events)
        page = await handler.run(ListEvents(limit=2))
        # Service asked for limit+1; surplus row trimmed, has_more set.
        handler.event_log.list_events.assert_awaited_once()
        assert handler.event_log.list_events.await_args.kwargs["limit"] == 3
        assert len(page.events) == 2
        assert page.has_more is True
        assert page.cursor == str(events[1].id)

    async def test_last_page_has_no_more(self):
        events = [_event("only")]
        page = await _handler(events).run(ListEvents(limit=2))
        assert len(page.events) == 1
        assert page.has_more is False
        assert page.cursor == str(events[0].id)

    async def test_empty_page_has_no_cursor(self):
        page = await _handler([]).run(ListEvents(limit=2))
        assert page.events == [] and page.cursor is None and page.has_more is False

    async def test_payload_shaping(self):
        events = [_event("hello")]
        page = await _handler(events).run(ListEvents(limit=5))
        entry = page.events[0]
        assert entry.type == "SomethingHappened"
        assert entry.data == {"detail": "hello"}  # id/created_at excluded from payload
        assert entry.id == events[0].id

    async def test_order_and_type_filters_forwarded(self):
        handler = _handler([])
        await handler.run(ListEvents(limit=5, types=["RecordPublished"], order="desc"))
        kwargs = handler.event_log.list_events.await_args.kwargs
        assert kwargs["event_types"] == ["RecordPublished"]
        assert kwargs["newest_first"] is True
