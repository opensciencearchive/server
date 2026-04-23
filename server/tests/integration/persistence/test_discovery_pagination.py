"""Integration tests for discovery keyset pagination against real Postgres.

Regression coverage for a production bug where paginating past page 1 raised
``operator does not exist: timestamp with time zone < character varying``
because the cursor's sort value round-tripped through JSON as a plain string
and was bound as ``VARCHAR`` against the typed ``records.published_at`` column.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.discovery.model.value import SortOrder, decode_cursor, encode_cursor
from osa.infrastructure.persistence.adapter.discovery import PostgresDiscoveryReadStore
from osa.infrastructure.persistence.tables import records_table


async def _insert_record(session: AsyncSession, srn: str, published_at: datetime) -> None:
    await session.execute(
        records_table.insert().values(
            srn=srn,
            convention_srn="urn:osa:localhost:conv:test@1.0.0",
            schema_id="test",
            schema_version="1.0.0",
            source={"type": "test", "id": srn},
            metadata={},
            published_at=published_at,
        )
    )
    await session.commit()


@pytest.mark.asyncio
class TestDiscoveryPaginationPublishedAt:
    async def test_second_page_with_published_at_cursor(self, pg_session: AsyncSession) -> None:
        """Fetching page 2 with a cursor must not trip the timestamptz/varchar
        mismatch — the bug manifested only on requests that supplied a cursor."""
        store = PostgresDiscoveryReadStore(pg_session)
        base = datetime(2026, 4, 7, 9, 0, 0, tzinfo=UTC)
        records = [(f"urn:osa:localhost:rec:page-{i}@1", base.replace(second=i)) for i in range(3)]
        for srn, ts in records:
            await _insert_record(pg_session, srn, ts)

        first_page = await store.search_records(
            filter_expr=None,
            schema_id=None,
            convention_srn=None,
            text_fields=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=2,
        )
        assert len(first_page) == 2

        # Encode + decode the cursor the same way the service does — this is the
        # round-trip that previously produced a VARCHAR bind.
        last = first_page[-1]
        cursor_str = encode_cursor(last.published_at.isoformat(), str(last.srn))
        decoded = decode_cursor(cursor_str)

        second_page = await store.search_records(
            filter_expr=None,
            schema_id=None,
            convention_srn=None,
            text_fields=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=decoded,
            limit=2,
        )

        assert len(second_page) == 1
        returned = {str(r.srn) for r in first_page} | {str(r.srn) for r in second_page}
        assert returned == {srn for srn, _ in records}
