"""Unit tests for PostgresFeatureStore — DDL generation, catalog registration, bulk insert."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookLimits,
    HookManifest,
)
from osa.infrastructure.persistence.feature_store import (
    FEATURES_SCHEMA,
    PostgresFeatureStore,
)


def _make_hook(
    name: str = "pocket_detect",
    columns: list[ColumnDef] | None = None,
) -> HookDefinition:
    if columns is None:
        columns = [
            ColumnDef(name="score", json_type="number", required=True),
            ColumnDef(name="pocket_id", json_type="string", required=True),
            ColumnDef(name="volume", json_type="number", required=False),
        ]
    return HookDefinition(
        image="ghcr.io/example/hook:v1",
        digest="sha256:abc123",
        manifest=HookManifest(
            name=name,
            record_schema="Sample",
            cardinality="many",
            feature_schema=FeatureSchema(columns=columns),
        ),
        limits=HookLimits(),
    )


def _mock_engine():
    """Create a mock AsyncEngine with a working begin() async context manager."""
    engine = AsyncMock()
    conn = AsyncMock()

    @asynccontextmanager
    async def mock_begin():
        yield conn

    engine.begin = mock_begin
    return engine, conn


class TestCreateTable:
    @pytest.mark.asyncio
    async def test_creates_features_schema(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_hook())

        schema_call = conn.execute.call_args_list[0]
        sql_text = str(schema_call[0][0].text)
        assert "CREATE SCHEMA IF NOT EXISTS" in sql_text
        assert FEATURES_SCHEMA in sql_text

    @pytest.mark.asyncio
    async def test_creates_table_via_metadata(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_hook())

        conn.run_sync.assert_called_once()

    @pytest.mark.asyncio
    async def test_registers_in_catalog(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_hook())

        # Last execute call is the catalog insert
        catalog_call = conn.execute.call_args_list[-1]
        insert_stmt = catalog_call[0][0]
        compiled = insert_stmt.compile()
        assert "feature_tables" in str(compiled)

    @pytest.mark.asyncio
    async def test_catalog_contains_hook_name(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_hook())

        # Catalog insert is the last execute call
        catalog_call = conn.execute.call_args_list[-1]
        insert_stmt = catalog_call[0][0]
        compiled = insert_stmt.compile()
        params = compiled.params
        assert params["hook_name"] == "pocket_detect"
        assert params["pg_table"] == "pocket_detect"
        assert params["schema_version"] == 1


class TestInsertFeatures:
    @pytest.mark.asyncio
    async def test_inserts_rows(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())
        rows = [
            {"score": 0.95, "pocket_id": "P1"},
            {"score": 0.82, "pocket_id": "P2"},
        ]

        count = await store.insert_features("pocket_detect", "urn:rec:1", rows)

        assert count == 2
        conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_rows_returns_zero(self):
        engine = AsyncMock()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        count = await store.insert_features("pocket_detect", "urn:rec:1", [])

        assert count == 0

    @pytest.mark.asyncio
    async def test_enriches_rows_with_record_srn(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.insert_features("pocket_detect", "urn:rec:1", [{"score": 0.95}])

        call_args = conn.execute.call_args
        params = call_args[0][1]  # second positional arg is the params list
        assert len(params) == 1
        assert params[0]["record_srn"] == "urn:rec:1"
        assert "created_at" in params[0]
        assert params[0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_chunks_large_inserts(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())
        rows = [{"score": float(i)} for i in range(2500)]

        count = await store.insert_features("hook", "urn:rec:1", rows)

        assert count == 2500
        assert conn.execute.call_count == 3  # 1000 + 1000 + 500

    @pytest.mark.asyncio
    async def test_single_chunk_for_small_batch(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())
        rows = [{"score": float(i)} for i in range(999)]

        count = await store.insert_features("hook", "urn:rec:1", rows)

        assert count == 999
        assert conn.execute.call_count == 1

    def test_builds_correct_insert_sql(self):
        engine = AsyncMock()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        sql = store._build_insert_sql("pocket_detect", ["record_srn", "created_at", "score"])

        assert f'"{FEATURES_SCHEMA}"."pocket_detect"' in sql
        assert '"record_srn", "created_at", "score"' in sql
        assert ":record_srn, :created_at, :score" in sql

    @pytest.mark.asyncio
    async def test_uses_features_schema(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.insert_features("hook", "urn:rec:1", [{"score": 0.95}])

        call_args = conn.execute.call_args
        sql_text = str(call_args[0][0].text)
        assert f'"{FEATURES_SCHEMA}"' in sql_text
