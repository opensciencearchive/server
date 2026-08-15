"""Nullable-column sort semantics pinned across #219 phase 3.

NOT NULL sort columns switch to PG-default NULLS emission (index-servable);
genuinely nullable columns — dynamic metadata fields — MUST keep explicit
``NULLS LAST`` in both directions and the OR-form keyset predicate, because
row-value comparison is not equivalent in the presence of NULLs. Absent
values sort last either way, and pagination crosses the NULL boundary without
gaps or duplicates.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.query_plan import (
    BoundedPage,
    PaginationCursor,
    QueryPlan,
    SortDirection,
    SortSpec,
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

# rid → mw (None = absent). Three non-null values and two NULLs.
_ROWS: list[tuple[str, float | None]] = [
    ("rec0", 3.0),
    ("rec1", None),
    ("rec2", 1.0),
    ("rec3", None),
    ("rec4", 2.0),
]


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="mw",
            type=FieldType.NUMBER,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _seed(engine: AsyncEngine, session: AsyncSession) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    for i, (rid, mw) in enumerate(_ROWS):
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
        meta: dict = {"species": f"sp{i}"}
        if mw is not None:
            meta["mw"] = mw
        await seed_record(
            engine,
            srn=str(srn),
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            metadata=meta,
            published_at=datetime(2026, 1, 1, 12, i, tzinfo=UTC),
        )
        await store.insert(SCHEMA, srn, meta)
    await session.commit()


def _plan(direction: SortDirection, limit: int = 10, cursor: str | None = None) -> QueryPlan:
    return QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        pagination=BoundedPage(
            limit=limit, cursor=PaginationCursor(value=cursor) if cursor else None
        ),
        sort=[SortSpec(column="mw", direction=direction)],
    )


async def _walk(rs: PostgresTableReadStore, direction: SortDirection, limit: int) -> list[str]:
    seen: list[str] = []
    cursor: str | None = None
    for _ in range(10):
        plan = _plan(direction, limit=limit, cursor=cursor)
        page = await plan.take_page(rs.stream_rows(plan))
        seen.extend(r["id"] for r in page.rows)
        if not page.truncated:
            break
        cursor = page.next_cursor
    return seen


@pytest.mark.asyncio
class TestNullableSortSemantics:
    async def test_absent_values_sort_last_ascending(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rows = await _walk(PostgresTableReadStore(pg_session), SortDirection.ASC, limit=10)
        # 1.0, 2.0, 3.0 then the two NULLs (tiebreak srn asc within regions).
        assert rows[:3] == ["rec2", "rec4", "rec0"]
        assert set(rows[3:]) == {"rec1", "rec3"}

    async def test_absent_values_sort_last_descending(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rows = await _walk(PostgresTableReadStore(pg_session), SortDirection.DESC, limit=10)
        assert rows[:3] == ["rec0", "rec4", "rec2"]
        assert set(rows[3:]) == {"rec1", "rec3"}

    async def test_pagination_crosses_the_null_boundary_without_gaps(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rs = PostgresTableReadStore(pg_session)
        for direction in (SortDirection.ASC, SortDirection.DESC):
            rows = await _walk(rs, direction, limit=2)
            assert len(rows) == 5 and len(set(rows)) == 5, (direction, rows)
