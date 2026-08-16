"""EXPLAIN assertions for the default-sort bounded reads (#219 phase 3).

The planner matches sort orderings textually: ``DESC NULLS LAST`` matches no
default btree in either scan direction, so the always-emitted NULLS LAST
guaranteed a Sort node — a pipeline breaker that materializes the entire
joined result before the first row. With PG-default NULLS emission on NOT NULL
sort columns, the composite index ``records (schema_id, schema_version,
published_at, srn)``, and row-value cursor predicates, the default-sort page
must plan as a pure index scan: no Sort node, rows examined ≈ limit+1.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.query_plan import BoundedPage, QueryPlan, TableKind
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.ids import FeatureName
from osa.domain.shared.model.srn import SchemaId
from osa.infrastructure.data.postgres_table_read_store import PostgresTableReadStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

from tests.factories import make_convention_docs_dict
from tests.integration.conftest import seed_hook_run

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


async def _setup(engine: AsyncEngine, session: AsyncSession, n: int) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    await session.commit()
    # Bulk set-based seeding: planner assertions need row volume, and per-row
    # inserts at this count dominate the test's wall clock.
    async with engine.begin() as conn:
        await conn.execute(
            sa.text(
                """
                INSERT INTO records (srn, convention_id, schema_id, schema_version,
                                     source, metadata, published_at)
                SELECT 'urn:osa:localhost:rec:rec' || lpad(g::text, 4, '0') || '@1',
                       'urn:osa:localhost:conv:test@1.0.0', :sid, :sver,
                       jsonb_build_object('type', 'seed', 'id', g::text),
                       jsonb_build_object('species', 'sp' || g),
                       TIMESTAMPTZ '2026-01-01' + g * INTERVAL '1 minute'
                FROM generate_series(0, :n - 1) g
                """
            ),
            {"sid": SCHEMA.id.root, "sver": SCHEMA.version.root, "n": n},
        )
        await conn.execute(
            sa.text(
                """
                INSERT INTO metadata.compound_v1 (record_srn, species)
                SELECT 'urn:osa:localhost:rec:rec' || lpad(g::text, 4, '0') || '@1',
                       'sp' || g
                FROM generate_series(0, :n - 1) g
                """
            ),
            {"n": n},
        )


async def _captured_page_query(
    engine: AsyncEngine, session: AsyncSession, plan: QueryPlan
) -> tuple[str, Any]:
    """Run the store's read and capture the page SELECT + its bind parameters."""
    captured: list[tuple[str, Any]] = []

    def _capture(conn, cursor, statement, parameters, context, executemany) -> None:
        if statement.lstrip().upper().startswith("SELECT") and "FROM records" in statement:
            captured.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", _capture)
    try:
        async for _row in PostgresTableReadStore(session).stream_rows(plan):
            pass
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _capture)
    assert captured, "no page SELECT captured"
    return captured[-1]


def _node_types(plan_node: dict, acc: list[str]) -> list[str]:
    acc.append(plan_node["Node Type"])
    for child in plan_node.get("Plans", []):
        _node_types(child, acc)
    return acc


async def _explain(engine: AsyncEngine, stmt: str, params: Any) -> dict:
    async with engine.connect() as conn:
        result = await conn.exec_driver_sql("EXPLAIN (ANALYZE, FORMAT JSON) " + stmt, params)
        payload = result.scalar_one()
    return payload[0]["Plan"]


@pytest.mark.asyncio
class TestDefaultSortPlansAsIndexScan:
    async def test_records_default_sort_has_no_sort_node(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup(pg_engine, pg_session, 300)
        plan = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.RECORDS,
            pagination=BoundedPage(limit=10),
        )
        stmt, params = await _captured_page_query(pg_engine, pg_session, plan)
        top = await _explain(pg_engine, stmt, params)
        nodes = _node_types(top, [])

        assert "Sort" not in nodes and "Incremental Sort" not in nodes, (
            f"default-sort bounded read still requires a Sort: {nodes}"
        )
        assert any("Index Scan" in n for n in nodes), (
            f"expected an index scan over records, got: {nodes}"
        )
        # O(page), not O(table): the whole plan examined ≈ limit+1 rows.
        assert top["Actual Rows"] <= 11

    async def test_cursor_page_also_plans_without_sort(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """The keyset predicate must collapse to one index range (row-value
        form) — the OR-form defeats the scan even with the right index."""
        await _setup(pg_engine, pg_session, 300)
        rs = PostgresTableReadStore(pg_session)
        first = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.RECORDS,
            pagination=BoundedPage(limit=10),
        )
        page = await first.take_page(rs.stream_rows(first))
        assert page.next_cursor is not None

        from osa.domain.data.model.query_plan import PaginationCursor

        follow = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.RECORDS,
            pagination=BoundedPage(limit=10, cursor=PaginationCursor(value=page.next_cursor)),
        )
        stmt, params = await _captured_page_query(pg_engine, pg_session, follow)
        top = await _explain(pg_engine, stmt, params)
        nodes = _node_types(top, [])
        assert "Sort" not in nodes and "Incremental Sort" not in nodes, (
            f"cursor-follow page still requires a Sort: {nodes}"
        )
        assert top["Actual Rows"] <= 11


HOOK = "chem_features"


@pytest.mark.asyncio
class TestFeatureDefaultSortNeedsNoNewIndex:
    """#219 explicitly does NOT add feature-table indexes — this verifies (not
    assumes) that the default feature read (id ASC = the PK) plans without a
    Sort using only the indexes the tables already have."""

    async def test_feature_default_sort_has_no_sort_node(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup(pg_engine, pg_session, 2000)
        await pg_session.execute(
            conventions_table.insert().values(
                id=f"{SCHEMA.id.root}-{HOOK}",
                title="conv",
                description="convention",
                schema_id=SCHEMA.id.root,
                schema_version=SCHEMA.version.root,
                file_requirements={},
                hooks=[HOOK],
                source=None,
                docs=make_convention_docs_dict(),
                created_at=datetime.now(UTC),
            )
        )
        await pg_session.commit()
        columns = [ColumnDef(name="score", json_type="number", required=True)]
        run_id = await seed_hook_run(pg_engine, feature_name=HOOK, columns=columns)
        await PostgresFeatureStore(pg_engine, pg_session).create_table(HOOK, columns)
        async with pg_engine.begin() as conn:
            await conn.execute(
                sa.text(
                    f"""
                    INSERT INTO features."{HOOK}" (record_srn, run_id, score)
                    SELECT 'urn:osa:localhost:rec:rec' || lpad((g / 2)::text, 4, '0') || '@1',
                           :run_id, g::float
                    FROM generate_series(0, 3999) g
                    """
                ),
                {"run_id": run_id},
            )
            # Planner choices at toy scale are costing noise; give it real stats.
            await conn.execute(sa.text("ANALYZE records"))
            await conn.execute(sa.text(f'ANALYZE features."{HOOK}"'))

        plan = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.FEATURE,
            feature_name=FeatureName(HOOK),
            pagination=BoundedPage(limit=10),
        )
        stmt, params = await _captured_feature_query(pg_engine, pg_session, plan)
        top = await _explain(pg_engine, stmt, params)
        nodes = _node_types(top, [])
        assert "Sort" not in nodes and "Incremental Sort" not in nodes, (
            f"feature default sort requires a Sort — a new index may be needed "
            f"(record findings on #219 before adding one): {nodes}"
        )


async def _captured_feature_query(
    engine: AsyncEngine, session: AsyncSession, plan: QueryPlan
) -> tuple[str, Any]:
    captured: list[tuple[str, Any]] = []

    def _capture(conn, cursor, statement, parameters, context, executemany) -> None:
        if statement.lstrip().upper().startswith("SELECT") and "features." in statement:
            captured.append((statement, parameters))

    event.listen(engine.sync_engine, "before_cursor_execute", _capture)
    try:
        async for _row in PostgresTableReadStore(session).stream_rows(plan):
            pass
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _capture)
    assert captured, "no feature SELECT captured"
    return captured[-1]
