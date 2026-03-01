"""Unit tests for PostgresFeatureStore — DDL generation, catalog registration, bulk insert."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa

from osa.domain.shared.error import ConflictError, ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.feature_store import (
    FEATURES_SCHEMA,
    PostgresFeatureStore,
)


def _make_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="score", json_type="number", required=True),
        ColumnDef(name="pocket_id", json_type="string", required=True),
        ColumnDef(name="volume", json_type="number", required=False),
    ]


def _mock_engine():
    """Create a mock AsyncEngine with a working begin() async context manager."""
    engine = AsyncMock()
    conn = AsyncMock()

    @asynccontextmanager
    async def mock_begin():
        yield conn

    engine.begin = mock_begin
    return engine, conn


def _mock_engine_with_reflect(table_name: str, feature_columns: list[str] | None = None):
    """Create a mock AsyncEngine where run_sync simulates table reflection.

    The reflected table will have id, record_srn, created_at plus any feature columns.
    """
    engine, conn = _mock_engine()

    def fake_run_sync(fn, *args, **kwargs):
        """Simulate metadata.reflect by registering a table on the metadata object."""
        # fn is metadata.reflect, first arg of fn is the sync connection (irrelevant in mock)
        # We need to find the MetaData that called reflect and register a table on it.
        # run_sync calls fn(sync_conn, *args) — metadata.reflect is a bound method,
        # so fn.__self__ is the MetaData object.
        metadata = fn.__self__
        cols = [
            sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
            sa.Column("record_srn", sa.Text, nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        ]
        for col_name in feature_columns or []:
            cols.append(sa.Column(col_name, sa.Float))
        sa.Table(table_name, metadata, *cols)

    conn.run_sync = AsyncMock(side_effect=fake_run_sync)
    return engine, conn


class TestCreateTable:
    @pytest.mark.asyncio
    async def test_creates_features_schema(self):
        engine, conn = _mock_engine()
        # Mock catalog check to return no existing row
        mock_result = MagicMock()
        mock_result.first.return_value = None
        conn.execute.return_value = mock_result
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_columns())

        schema_call = conn.execute.call_args_list[0]
        sql_text = str(schema_call[0][0].text)
        assert "CREATE SCHEMA IF NOT EXISTS" in sql_text
        assert FEATURES_SCHEMA in sql_text

    @pytest.mark.asyncio
    async def test_creates_table_via_metadata(self):
        engine, conn = _mock_engine()
        mock_result = MagicMock()
        mock_result.first.return_value = None
        conn.execute.return_value = mock_result
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_columns())

        conn.run_sync.assert_called_once()

    @pytest.mark.asyncio
    async def test_registers_in_catalog(self):
        engine, conn = _mock_engine()
        mock_result = MagicMock()
        mock_result.first.return_value = None
        conn.execute.return_value = mock_result
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_columns())

        # Last execute call is the catalog insert
        catalog_call = conn.execute.call_args_list[-1]
        insert_stmt = catalog_call[0][0]
        compiled = insert_stmt.compile()
        assert "feature_tables" in str(compiled)

    @pytest.mark.asyncio
    async def test_catalog_contains_hook_name(self):
        engine, conn = _mock_engine()
        mock_result = MagicMock()
        mock_result.first.return_value = None
        conn.execute.return_value = mock_result
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.create_table("pocket_detect", _make_columns())

        # Catalog insert is the last execute call
        catalog_call = conn.execute.call_args_list[-1]
        insert_stmt = catalog_call[0][0]
        compiled = insert_stmt.compile()
        params = compiled.params
        assert params["hook_name"] == "pocket_detect"
        assert params["pg_table"] == "pocket_detect"
        assert params["schema_version"] == 1

    @pytest.mark.asyncio
    async def test_create_table_raises_conflict_on_duplicate(self):
        engine, conn = _mock_engine()
        # Mock catalog check to return an existing row
        mock_result = MagicMock()
        mock_result.first.return_value = ("pocket_detect",)
        conn.execute.return_value = mock_result
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        with pytest.raises(ConflictError, match="already exists"):
            await store.create_table("pocket_detect", _make_columns())


class TestInsertFeatures:
    @pytest.mark.asyncio
    async def test_inserts_rows(self):
        engine, conn = _mock_engine_with_reflect("pocket_detect", ["score", "pocket_id"])
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
        engine, conn = _mock_engine_with_reflect("pocket_detect", ["score"])
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
        engine, conn = _mock_engine_with_reflect("hook", ["score"])
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())
        rows = [{"score": float(i)} for i in range(2500)]

        count = await store.insert_features("hook", "urn:rec:1", rows)

        assert count == 2500
        assert conn.execute.call_count == 3  # 1000 + 1000 + 500

    @pytest.mark.asyncio
    async def test_single_chunk_for_small_batch(self):
        engine, conn = _mock_engine_with_reflect("hook", ["score"])
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())
        rows = [{"score": float(i)} for i in range(999)]

        count = await store.insert_features("hook", "urn:rec:1", rows)

        assert count == 999
        assert conn.execute.call_count == 1

    @pytest.mark.asyncio
    async def test_insert_rejects_invalid_hook_name(self):
        engine = AsyncMock()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        with pytest.raises(ValidationError, match="Invalid identifier"):
            await store.insert_features("'; DROP TABLE --", "urn:rec:1", [{"score": 1}])

    @pytest.mark.asyncio
    async def test_create_rejects_invalid_hook_name(self):
        engine, conn = _mock_engine()
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        with pytest.raises(ValidationError, match="Invalid identifier"):
            await store.create_table("'; DROP TABLE --", _make_columns())

    @pytest.mark.asyncio
    async def test_reflects_table_before_insert(self):
        """insert_features reflects the real table schema instead of guessing types."""
        engine, conn = _mock_engine_with_reflect("hook", ["score"])
        store = PostgresFeatureStore(engine=engine, session=AsyncMock())

        await store.insert_features("hook", "urn:rec:1", [{"score": 0.95}])

        # run_sync should have been called for reflection
        conn.run_sync.assert_called_once()
