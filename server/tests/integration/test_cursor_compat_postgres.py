"""Cursor wire-compatibility across the #219 predicate change (phase 3).

Live consumers hold ``next_cursor`` tokens across deploys. The cursor payload
(``{"s": sort_value, "id": tiebreak}``, urlsafe base64) is wire contract; only
predicate *compilation* may change. These tests mint cursors exactly as a
pre-#219 server handed them out — including the datetime-as-ISO-string
rendering of ``encode_cursor`` — and prove pagination resumes correctly
(no gaps, no duplicates) through the current predicate path.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.query_plan import (
    BoundedPage,
    PaginationCursor,
    QueryPlan,
    TableKind,
    encode_cursor,
)
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.data.postgres_table_read_store import PostgresTableReadStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)

from tests.integration.conftest import seed_record

SCHEMA = SchemaId.parse("compound@1.0.0")

# Deterministic publish times: rec0 oldest … rec9 newest. Default records sort
# is published_at DESC, srn DESC, so the first page is rec9, rec8, …
_PUBLISHED = [datetime(2026, 1, 1, 12, i, tzinfo=UTC) for i in range(10)]


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _seed(engine: AsyncEngine, session: AsyncSession) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    for i, published in enumerate(_PUBLISHED):
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:rec{i}@1")
        await seed_record(
            engine,
            srn=str(srn),
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            metadata={"species": f"sp{i}"},
            published_at=published,
        )
        await store.insert(SCHEMA, srn, {"species": f"sp{i}"})
    await session.commit()


@pytest.mark.asyncio
class TestPreChangeCursorsStillPaginate:
    async def test_cursor_minted_by_a_pre_219_server_resumes_correctly(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rs = PostgresTableReadStore(pg_session)

        # A pre-#219 server encoded the last row of page one — rec7 at
        # position 3 of the DESC ordering — with default=str datetime
        # rendering. Mint that token byte-for-byte, bypassing today's
        # page-taking machinery entirely.
        legacy_cursor = encode_cursor(_PUBLISHED[7], "urn:osa:localhost:rec:rec7@1")

        plan = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.RECORDS,
            pagination=BoundedPage(limit=4, cursor=PaginationCursor(value=legacy_cursor)),
        )
        page = await plan.take_page(rs.stream_rows(plan))
        ids = [r["id"] for r in page.rows]
        # Strictly after rec7 in DESC order: rec6..rec3, no gap, no repeat.
        assert ids == ["rec6", "rec5", "rec4", "rec3"]
        assert page.truncated and page.next_cursor is not None

    async def test_full_walk_from_legacy_cursor_covers_the_tail_exactly_once(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rs = PostgresTableReadStore(pg_session)
        cursor = encode_cursor(_PUBLISHED[7], "urn:osa:localhost:rec:rec7@1")

        seen: list[str] = []
        for _ in range(10):
            plan = QueryPlan(
                schema_id=SCHEMA,
                table_kind=TableKind.RECORDS,
                pagination=BoundedPage(limit=3, cursor=PaginationCursor(value=cursor)),
            )
            page = await plan.take_page(rs.stream_rows(plan))
            seen.extend(r["id"] for r in page.rows)
            if not page.truncated:
                break
            assert page.next_cursor is not None
            cursor = page.next_cursor
        assert seen == ["rec6", "rec5", "rec4", "rec3", "rec2", "rec1", "rec0"]
