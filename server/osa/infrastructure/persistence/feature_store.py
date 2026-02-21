"""PostgreSQL implementation of FeatureStore — dynamic DDL and bulk insert."""

from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.feature.port.feature_store import FeatureStore
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.tables import feature_tables_table


def _pg_schema_name(convention_id: str) -> str:
    """Derive PG schema name from convention ID."""
    return f"hook_{convention_id.lower()}"


class PostgresFeatureStore(FeatureStore):
    """Manages feature tables using dynamic DDL via SQLAlchemy Core."""

    def __init__(self, engine: AsyncEngine, session: AsyncSession) -> None:
        self._engine = engine
        self._session = session

    async def create_tables(self, convention_id: str, hooks: list[HookDefinition]) -> None:
        pg_schema = _pg_schema_name(convention_id)

        async with self._engine.begin() as conn:
            # Create PG schema
            quoted = pg_schema.replace('"', '""')
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{quoted}"'))

            for hook_def in hooks:
                table_name = hook_def.manifest.name

                # Build dynamic table
                metadata = sa.MetaData(schema=pg_schema)
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

                for col_def in hook_def.manifest.feature_schema.columns:
                    columns.append(map_column(col_def))

                # sa.Table registers itself on the metadata object
                sa.Table(table_name, metadata, *columns)

                # Create table (idempotent)
                await conn.run_sync(metadata.create_all, checkfirst=True)

                # Register in catalog
                await conn.execute(
                    insert(feature_tables_table)
                    .values(
                        convention_id=convention_id,
                        hook_name=hook_def.manifest.name,
                        pg_schema=pg_schema,
                        pg_table=table_name,
                        feature_schema=hook_def.manifest.feature_schema.model_dump(),
                        schema_version=1,
                        created_at=datetime.now(UTC),
                    )
                    .on_conflict_do_nothing()
                )

    async def insert_features(
        self,
        convention_id: str,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        pg_schema = _pg_schema_name(convention_id)
        now = datetime.now(UTC)
        enriched_rows = [{"record_srn": record_srn, "created_at": now, **row} for row in rows]

        # Bulk insert in chunks of 1000
        chunk_size = 1000
        total = 0
        async with self._engine.begin() as conn:
            for i in range(0, len(enriched_rows), chunk_size):
                chunk = enriched_rows[i : i + chunk_size]
                await conn.execute(
                    text(self._build_insert_sql(pg_schema, hook_name, chunk[0].keys())),
                    chunk,
                )
                total += len(chunk)
        return total

    def _build_insert_sql(self, pg_schema: str, table_name: str, columns: Any) -> str:
        """Build a parameterized INSERT statement."""
        cols = ", ".join(f'"{c}"' for c in columns)
        params = ", ".join(f":{c}" for c in columns)
        return f'INSERT INTO "{pg_schema}"."{table_name}" ({cols}) VALUES ({params})'
