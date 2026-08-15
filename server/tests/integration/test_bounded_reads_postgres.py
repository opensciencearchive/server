"""LIMIT pushdown for bounded table reads (#219 phase 2).

A ``BoundedPage`` read must compile its limit into SQL (``LIMIT limit+1``) so
Postgres can top-N instead of sorting the entire joined result; the previous
plan carried a limit the store ignored, and ``take_page`` discarded the surplus
in Python over a server-side cursor. ``FullStream`` (the dump path) must remain
genuinely unbounded.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.query_plan import (
    BoundedPage,
    FullStream,
    PaginationCursor,
    QueryPlan,
    TableKind,
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


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _setup_schema(engine: AsyncEngine, session: AsyncSession) -> PostgresMetadataStore:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    return store


async def _seed(engine: AsyncEngine, store: PostgresMetadataStore, n: int) -> None:
    for i in range(n):
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:rec{i:03d}@1")
        await seed_record(
            engine,
            srn=str(srn),
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            metadata={"species": f"sp{i}"},
            published_at=datetime(2026, 1, 1, i % 24, i % 60, tzinfo=UTC),
        )
        await store.insert(SCHEMA, srn, {"species": f"sp{i}"})


def _plan(pagination) -> QueryPlan:
    return QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS, pagination=pagination)


async def _drain(store: PostgresTableReadStore, plan: QueryPlan) -> list[dict]:
    return [dict(row) async for row in store.stream_rows(plan)]


@pytest.mark.asyncio
class TestLimitPushdown:
    async def test_bounded_read_compiles_limit_into_sql(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _seed(pg_engine, store, 8)
        await pg_session.commit()

        captured_sql.clear()
        rows = await _drain(PostgresTableReadStore(pg_session), _plan(BoundedPage(limit=3)))

        selects = [s for s in captured_sql if s.lstrip().upper().startswith("SELECT")]
        page_selects = [s for s in selects if "records" in s]
        assert page_selects, f"no page SELECT captured: {captured_sql}"
        assert any("LIMIT" in s.upper() for s in page_selects), (
            f"bounded read did not push LIMIT into SQL: {page_selects}"
        )
        # limit+1: exactly one look-ahead row beyond the page, never the table.
        assert len(rows) == 4

    async def test_full_stream_is_unbounded_and_has_no_limit(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _seed(pg_engine, store, 8)
        await pg_session.commit()

        captured_sql.clear()
        rows = await _drain(PostgresTableReadStore(pg_session), _plan(FullStream()))

        assert len(rows) == 8
        page_selects = [
            s for s in captured_sql if s.lstrip().upper().startswith("SELECT") and "records" in s
        ]
        assert all("LIMIT" not in s.upper() for s in page_selects), (
            f"FullStream must not carry a LIMIT: {page_selects}"
        )


@pytest.mark.asyncio
class TestPageBoundaries:
    """take_page semantics pinned across the pushdown (was Python-side truncation)."""

    async def test_exact_fill_has_cursor_iff_more_rows(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _seed(pg_engine, store, 5)
        await pg_session.commit()
        rs = PostgresTableReadStore(pg_session)

        plan = _plan(BoundedPage(limit=3))
        page = await plan.take_page(rs.stream_rows(plan))
        assert len(page.rows) == 3
        assert page.truncated and page.next_cursor is not None

        plan2 = _plan(BoundedPage(limit=2, cursor=PaginationCursor(value=page.next_cursor)))
        page2 = await plan2.take_page(rs.stream_rows(plan2))
        assert len(page2.rows) == 2
        assert not page2.truncated and page2.next_cursor is None

    async def test_empty_page_has_no_cursor(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresTableReadStore(pg_session)
        plan = _plan(BoundedPage(limit=3))
        page = await plan.take_page(rs.stream_rows(plan))
        assert page.rows == [] and page.next_cursor is None and not page.truncated

    async def test_pagination_covers_all_rows_without_gaps_or_dupes(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _seed(pg_engine, store, 7)
        await pg_session.commit()
        rs = PostgresTableReadStore(pg_session)

        seen: list[str] = []
        cursor: str | None = None
        for _ in range(10):
            plan = _plan(
                BoundedPage(
                    limit=3,
                    cursor=PaginationCursor(value=cursor) if cursor else None,
                )
            )
            page = await plan.take_page(rs.stream_rows(plan))
            seen.extend(r["srn"] for r in page.rows)
            if not page.truncated:
                break
            cursor = page.next_cursor
        assert len(seen) == 7 and len(set(seen)) == 7
