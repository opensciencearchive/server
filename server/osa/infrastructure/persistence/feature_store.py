"""PostgreSQL implementation of FeatureStore — dynamic DDL and bulk insert."""

import json
import re
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.shared.error import ConflictError, ValidationError
from osa.domain.shared.model.hook import HookDefinition
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.tables import feature_tables_table

FEATURES_SCHEMA = "features"

_PG_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def _validate_pg_identifier(name: str) -> str:
    """Validate a string is a safe PostgreSQL identifier."""
    if not _PG_IDENTIFIER.match(name):
        raise ValidationError(
            f"Invalid identifier: {name!r}. "
            "Must be lowercase alphanumeric/underscore, starting with a letter."
        )
    return name


class PostgresFeatureStore(FeatureStore):
    """Manages feature tables using dynamic DDL via SQLAlchemy Core.

    All feature tables live in a single ``features`` PG schema.
    Table name = hook name directly. Collision at create_table time is a hard error.
    """

    def __init__(self, engine: AsyncEngine, session: AsyncSession) -> None:
        self._engine = engine
        self._session = session

    async def create_table(self, hook_name: str, hook: HookDefinition) -> None:
        _validate_pg_identifier(hook_name)

        async with self._engine.begin() as conn:
            # Ensure the features schema exists
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{FEATURES_SCHEMA}"'))

            # Check for existing table in catalog — duplicate is a hard error
            existing = await conn.execute(
                select(feature_tables_table.c.hook_name).where(
                    feature_tables_table.c.hook_name == hook_name
                )
            )
            if existing.first() is not None:
                raise ConflictError(f"Feature table already exists: {hook_name}")

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

            # Create table
            await conn.run_sync(metadata.create_all, checkfirst=False)

            # Register in catalog
            await conn.execute(
                feature_tables_table.insert().values(
                    hook_name=hook_name,
                    pg_table=hook_name,
                    feature_schema=hook.manifest.feature_schema.model_dump(),
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )

    async def insert_features(
        self,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        if not rows:
            return 0

        _validate_pg_identifier(hook_name)

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
            # Reflect the actual table to get correct column types for casts
            metadata = sa.MetaData(schema=FEATURES_SCHEMA)
            await conn.run_sync(metadata.reflect, only=[hook_name])
            table = metadata.tables[f"{FEATURES_SCHEMA}.{hook_name}"]

            for i in range(0, len(enriched_rows), chunk_size):
                chunk = enriched_rows[i : i + chunk_size]
                await conn.execute(table.insert(), chunk)
                total += len(chunk)
        return total
