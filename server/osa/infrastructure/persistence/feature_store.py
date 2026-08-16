"""PostgreSQL implementation of FeatureStore — dynamic DDL and bulk insert."""

import json
import re
from datetime import UTC, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.shared.error import ConflictError, NotFoundError, ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.api_naming import feature_pg_schema, feature_pg_table
from osa.infrastructure.persistence.feature_table import (
    FeatureSchema,
    build_feature_table,
)
from osa.domain.shared.model.srn import SchemaId
from osa.infrastructure.persistence.statistics_upsert import (
    FeatureDelta,
    bump_table_statistics,
)
from osa.infrastructure.persistence.tables import feature_tables_table, records_table

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

    async def create_table(self, feature: str, columns: list[ColumnDef]) -> None:
        _validate_pg_identifier(feature)

        async with self._engine.begin() as conn:
            # Ensure the features schema exists
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{feature_pg_schema()}"'))

            # Check for existing table in catalog — duplicate is a hard error
            existing = await conn.execute(
                select(feature_tables_table.c.hook_name).where(
                    feature_tables_table.c.hook_name == feature
                )
            )
            if existing.first() is not None:
                raise ConflictError(f"Feature table already exists: {feature}")

            # Build dynamic table
            schema = FeatureSchema(columns=columns)
            table = build_feature_table(feature, schema)

            # Create table (FK to records.srn is declared inline on the column)
            await conn.run_sync(table.metadata.create_all, checkfirst=False)
            await conn.execute(
                feature_tables_table.insert().values(
                    hook_name=feature,
                    pg_table=feature_pg_table(feature),
                    feature_schema=schema.model_dump(),
                    schema_version=1,
                    created_at=datetime.now(UTC),
                )
            )

    async def insert_features(
        self,
        feature: str,
        record_srn: str,
        rows: list[dict[str, Any]],
        run_id: str,
    ) -> int:
        """Insert this record's feature rows with replace semantics per record.

        Redoing an insert after a partial failure converges instead of
        duplicating rows (#160): existing rows for ``record_srn`` in this
        feature table are deleted before the insert, in the same transaction.

        DML runs on the injected session (#219 phase 4) — the caller's unit of
        work owns commit/rollback, so records, metadata, and feature rows
        written in one stage land or vanish together. The table object comes
        from the ``feature_tables`` catalog (as the read path builds it);
        runtime reflection was the only reason this ever needed a raw engine
        connection.
        """
        if not rows:
            return 0

        _validate_pg_identifier(feature)
        table = await self._catalog_table(feature)

        now = datetime.now(UTC)
        enriched_rows = [
            {
                "record_srn": record_srn,
                "run_id": run_id,
                "created_at": now,
                **{k: json.dumps(v) if isinstance(v, (list, dict)) else v for k, v in row.items()},
            }
            for row in rows
        ]

        # Replace-by-record: drop any prior rows for this record so a redo
        # after a partial failure converges instead of duplicating (#160).
        delete_result = await self._session.execute(
            table.delete().where(table.c.record_srn == record_srn)
        )
        # DML always yields a CursorResult; the isinstance narrows the union
        # session.execute is typed with. max() guards the DBAPI's -1 sentinel.
        deleted = max(delete_result.rowcount, 0) if isinstance(delete_result, CursorResult) else 0

        chunk_size = 1000
        total = 0
        for i in range(0, len(enriched_rows), chunk_size):
            chunk = enriched_rows[i : i + chunk_size]
            await self._session.execute(table.insert(), chunk)
            total += len(chunk)

        # Lockstep statistics (#219 phase 5): replace-by-record yields exact
        # in-transaction deltas — rows = inserted − deleted; a record enters
        # coverage on its first feature write only. The schema identity comes
        # from the record row itself (feature tables are shared across
        # schemas), so attribution cannot drift from the data.
        schema = await self._record_schema(record_srn)
        await bump_table_statistics(
            self._session,
            schema=schema,
            delta=FeatureDelta(
                feature=feature,
                rows=total - deleted,
                covered=1 if deleted == 0 else 0,
            ),
        )
        await self._session.flush()
        return total

    async def _record_schema(self, record_srn: str) -> SchemaId:
        """The owning record's schema identity (PK lookup, in-transaction)."""
        result = await self._session.execute(
            select(records_table.c.schema_id, records_table.c.schema_version).where(
                records_table.c.srn == record_srn
            )
        )
        row = result.first()
        if row is None:
            raise NotFoundError(f"No record '{record_srn}' to attach feature rows to.")
        return SchemaId.parse(f"{row[0]}@{row[1]}")

    async def _catalog_table(self, feature: str) -> sa.Table:
        """Build the feature's table object from the ``feature_tables`` catalog."""
        result = await self._session.execute(
            select(feature_tables_table.c.feature_schema).where(
                feature_tables_table.c.hook_name == feature
            )
        )
        row = result.first()
        if row is None:
            raise NotFoundError(f"No feature table registered for hook '{feature}'.")
        return build_feature_table(feature, FeatureSchema.model_validate(row[0]))
