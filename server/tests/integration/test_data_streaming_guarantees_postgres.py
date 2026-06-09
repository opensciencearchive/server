"""Integration tests for the streaming engine's resource guarantees (T041, T042).

These exercise the two non-functional promises of the ``/data/`` streaming path
directly against the read store + a real Postgres server-side cursor:

* **Bounded memory (T041)** — streaming a large table must not materialize all
  rows in the Python heap. Asserted with ``tracemalloc`` peak while draining the
  dump without accumulating, against a bulk-seeded table. (The literal spec's
  absolute "<50 MB RSS" is not meaningful in-process — the test runner's RSS is
  already well above that from imports — so we assert *Python-heap peak stays
  small*, which is the guarantee the streaming primitive actually provides.)

* **Cursor release on early termination (T042)** — closing the async generator
  mid-stream (the client-disconnect path: a ``GeneratorExit`` thrown into the
  body) must run the ``try/finally`` that closes the server-side cursor, leaving
  the connection immediately reusable. (Simulating a real TCP disconnect +
  ``pg_stat_activity`` poll is not reliable through the in-process ASGI client,
  so we drive the generator's lifecycle directly, which is where the guarantee
  lives.)

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

import tracemalloc
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.query_plan import PaginationParams, QueryPlan, TableKind
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import Domain, SchemaId
from osa.infrastructure.data.postgres_data_read_store import PostgresDataReadStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)

SCHEMA = SchemaId.parse("compound@1.0.0")


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


async def _setup_schema(engine: AsyncEngine, session: AsyncSession) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    await session.commit()


async def _bulk_seed(engine: AsyncEngine, n: int) -> None:
    """Insert *n* records + metadata rows in two set-based statements.

    ``source.id`` is per-row unique to satisfy the records source uniqueness
    index; otherwise a single bulk INSERT would collide on the second row.
    """
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO records
                    (srn, convention_srn, schema_id, schema_version, source, metadata, published_at)
                SELECT 'urn:osa:localhost:rec:bulk' || g || '@1',
                       'urn:osa:localhost:conv:test@1.0.0', 'compound', '1.0.0',
                       jsonb_build_object('type', 'deposition', 'id', 'bulk' || g),
                       jsonb_build_object('species', 'Homo sapiens', 'mw', g),
                       now()
                FROM generate_series(1, :n) g
                """
            ),
            {"n": n},
        )
        await conn.execute(
            text(
                """
                INSERT INTO metadata.compound_v1 (record_srn, species, mw)
                SELECT 'urn:osa:localhost:rec:bulk' || g || '@1', 'Homo sapiens', g
                FROM generate_series(1, :n) g
                """
            ),
            {"n": n},
        )


def _records_plan(limit: int = 1000) -> QueryPlan:
    return QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        pagination=PaginationParams(limit=limit),
    )


@pytest.mark.asyncio
class TestStreamingGuarantees:
    async def test_large_dump_does_not_materialize_in_python_heap(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        n = 20_000
        await _setup_schema(pg_engine, pg_session)
        await _bulk_seed(pg_engine, n)
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))

        tracemalloc.start()
        count = 0
        # Drain WITHOUT accumulating — the engine must not hold all rows at once.
        async for _row in rs.stream_rows(_records_plan()):
            count += 1
        _current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        assert count == n
        # If the engine buffered all 20K flattened dicts, peak would be many MB.
        # A streaming server-side cursor keeps the Python-heap peak small.
        assert peak < 8 * 1024 * 1024, f"peak heap {peak} bytes — streaming may be buffering"

    async def test_early_generator_close_releases_server_cursor(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await _bulk_seed(pg_engine, 500)
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))

        gen = rs.stream_rows(_records_plan())
        first = await gen.__anext__()  # opens the server-side cursor
        assert first["schema_id"] == "compound@1.0.0"
        # Simulate client disconnect: closing the generator throws GeneratorExit
        # into the body, which must run the finally that closes the cursor.
        await gen.aclose()

        # If the cursor leaked (portal still open), a follow-up query on the same
        # connection would raise. A clean full drain proves the connection was
        # returned to a usable state.
        rows = [r async for r in rs.stream_rows(_records_plan())]
        assert len(rows) == 500
