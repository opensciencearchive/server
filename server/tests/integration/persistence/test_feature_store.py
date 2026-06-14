"""Integration tests for PostgresFeatureStore — DDL, JSONB columns, ON CONFLICT."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.shared.model.hook import (
    ColumnDef,
    TableFeatureSpec,
)
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.feature_table import FEATURES_SCHEMA


def _make_feature(
    columns: list[ColumnDef] | None = None,
) -> TableFeatureSpec:
    """A hook's output contract (#145: ``HookIdentity`` is name + feature only;
    runtime moved to ``HookDeploy``/``HookRelease``). The store only needs the
    feature columns, so build the ``TableFeatureSpec`` directly."""
    if columns is None:
        columns = [
            ColumnDef(name="score", json_type="number", required=True),
            ColumnDef(name="label", json_type="string", required=False),
        ]
    return TableFeatureSpec(
        cardinality="many",
        columns=columns,
    )


@pytest.mark.asyncio
class TestFeatureStoreCreateTable:
    async def test_create_table_creates_schema_and_table(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresFeatureStore(pg_engine, pg_session)
        feature = _make_feature()

        await store.create_table("integration_test_hook", feature.columns)

        # Verify the table exists in the features schema
        async with pg_engine.begin() as conn:
            result = await conn.execute(
                text(
                    "SELECT EXISTS ("
                    "  SELECT 1 FROM information_schema.tables "
                    "  WHERE table_schema = :schema AND table_name = :table"
                    ")"
                ),
                {"schema": FEATURES_SCHEMA, "table": "integration_test_hook"},
            )
            assert result.scalar() is True

            # Verify columns
            result = await conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "ORDER BY ordinal_position"
                ),
                {"schema": FEATURES_SCHEMA, "table": "integration_test_hook"},
            )
            cols = {row[0]: row[1] for row in result.fetchall()}
            assert "id" in cols
            assert "record_srn" in cols
            assert "created_at" in cols
            assert "score" in cols
            assert "label" in cols

    async def test_create_table_duplicate_raises_conflict(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """Calling create_table twice raises ConflictError."""
        from osa.domain.shared.error import ConflictError

        store = PostgresFeatureStore(pg_engine, pg_session)
        feature = _make_feature()

        await store.create_table("duplicate_hook", feature.columns)
        with pytest.raises(ConflictError, match="already exists"):
            await store.create_table("duplicate_hook", feature.columns)

    async def test_create_table_registers_in_catalog(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresFeatureStore(pg_engine, pg_session)
        feature = _make_feature()

        await store.create_table("catalog_hook", feature.columns)

        # Check catalog
        async with pg_engine.begin() as conn:
            result = await conn.execute(
                text("SELECT hook_name, pg_table FROM feature_tables WHERE hook_name = :name"),
                {"name": "catalog_hook"},
            )
            row = result.first()
            assert row is not None
            assert row[0] == "catalog_hook"
            assert row[1] == "catalog_hook"


@pytest.mark.asyncio
class TestFeatureStoreInsert:
    async def test_insert_features(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        from tests.integration.conftest import seed_hook_run, seed_record

        store = PostgresFeatureStore(pg_engine, pg_session)
        feature = _make_feature()
        await store.create_table("insert_hook", feature.columns)

        record_srn = "urn:osa:localhost:rec:rec-001@1"
        await seed_record(pg_engine, srn=record_srn)
        # Every feature row carries a NOT NULL run_id FK to hook_runs.id (#145).
        run_id = await seed_hook_run(pg_engine, feature_name="insert_hook")

        rows = [
            {"score": 0.95, "label": "good"},
            {"score": 0.42, "label": "poor"},
            {"score": 0.78, "label": None},
        ]
        count = await store.insert_features("insert_hook", record_srn, rows, run_id)
        assert count == 3

        # Verify data is in the table
        async with pg_engine.begin() as conn:
            result = await conn.execute(
                text(f'SELECT record_srn, score, label FROM "{FEATURES_SCHEMA}"."insert_hook"')
            )
            fetched = result.fetchall()
            assert len(fetched) == 3
            scores = {row[1] for row in fetched}
            assert scores == {0.95, 0.42, 0.78}

    async def test_insert_empty_rows_returns_zero(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresFeatureStore(pg_engine, pg_session)
        # Empty rows short-circuits before any insert, so the run_id is never
        # used (no FK check) — a placeholder is fine here.
        count = await store.insert_features(
            "whatever", "urn:osa:localhost:rec:x@1", [], "00000000-0000-0000-0000-000000000000"
        )
        assert count == 0


@pytest.mark.asyncio
class TestFeatureStoreJsonbColumns:
    async def test_jsonb_column_for_array_and_object(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """Array and object types should map to JSONB columns."""
        columns = [
            ColumnDef(name="tags", json_type="array", required=False),
            ColumnDef(name="metadata", json_type="object", required=False),
            ColumnDef(name="count", json_type="integer", required=True),
        ]
        feature = _make_feature(columns=columns)
        store = PostgresFeatureStore(pg_engine, pg_session)
        await store.create_table("jsonb_hook", feature.columns)

        # Verify JSONB columns via information_schema
        async with pg_engine.begin() as conn:
            result = await conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = :schema AND table_name = :table "
                    "AND column_name IN ('tags', 'metadata', 'count')"
                ),
                {"schema": FEATURES_SCHEMA, "table": "jsonb_hook"},
            )
            col_types = {row[0]: row[1] for row in result.fetchall()}
            assert col_types["tags"] == "jsonb"
            assert col_types["metadata"] == "jsonb"
            assert col_types["count"] == "bigint"

        from tests.integration.conftest import seed_hook_run, seed_record

        record_srn = "urn:osa:localhost:rec:rec-jsonb@1"
        await seed_record(pg_engine, srn=record_srn)
        run_id = await seed_hook_run(pg_engine, feature_name="jsonb_hook", columns=columns)

        # Insert data with JSONB values
        rows = [
            {
                "tags": ["a", "b", "c"],
                "metadata": {"key": "value", "nested": {"deep": True}},
                "count": 42,
            }
        ]
        count = await store.insert_features("jsonb_hook", record_srn, rows, run_id)
        assert count == 1
