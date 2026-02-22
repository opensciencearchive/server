"""PostgreSQL implementation of FeatureStore — dynamic DDL and bulk insert."""

import json
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.shared.model.hook import HookDefinition
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.tables import feature_tables_table

FEATURES_SCHEMA = "features"


class PostgresFeatureStore(FeatureStore):
    """Manages feature tables using dynamic DDL via SQLAlchemy Core.

    All feature tables live in a single ``features`` PG schema.
    Table name = hook name directly. Collision at create_table time is a hard error.
    """

    def __init__(self, engine: AsyncEngine, session: AsyncSession) -> None:
        self._engine = engine
        self._session = session

    async def create_table(self, hook_name: str, hook: HookDefinition) -> None:
        async with self._engine.begin() as conn:
            # Ensure the features schema exists
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{FEATURES_SCHEMA}"'))

            # Build dynamic table
            metadata = sa.MetaData(schema=FEATURES_SCHEMA)
            columns: list[sa.Column] = [
                sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
                sa.Column("record_srn", sa.Text, nullable=False, index=True),
                sa.Column(
                    "created_at",
                    sa.DateTime(timezone=True),
                    nullable=False,
                    server_default=sa.func.now(),
                ),
            ]

            for col_def in hook.manifest.feature_schema.columns:
                columns.append(map_column(col_def))

            # sa.Table registers itself on the metadata object
            sa.Table(hook_name, metadata, *columns)

            # Create table (idempotent)
            await conn.run_sync(metadata.create_all, checkfirst=True)

            # Register in catalog
            await conn.execute(
                insert(feature_tables_table)
                .values(
                    hook_name=hook_name,
                    pg_table=hook_name,
                    feature_schema=hook.manifest.feature_schema.model_dump(),
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
                .on_conflict_do_nothing()
            )

    async def insert_features(
        self,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        now = datetime.now(UTC)
        enriched_rows = [
            {
                "record_srn": record_srn,
                "created_at": now,
                **{k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in row.items()},
            }
            for row in rows
        ]

        # Bulk insert in chunks of 1000
        chunk_size = 1000
        total = 0
        async with self._engine.begin() as conn:
            for i in range(0, len(enriched_rows), chunk_size):
                chunk = enriched_rows[i : i + chunk_size]
                await conn.execute(
                    text(self._build_insert_sql(hook_name, chunk[0].keys())),
                    chunk,
                )
                total += len(chunk)
        return total

    def _build_insert_sql(self, table_name: str, columns: Any) -> str:
        """Build a parameterized INSERT statement."""
        cols = ", ".join(f'"{c}"' for c in columns)
        params = ", ".join(f":{c}" for c in columns)
        return f'INSERT INTO "{FEATURES_SCHEMA}"."{table_name}" ({cols}) VALUES ({params})'
