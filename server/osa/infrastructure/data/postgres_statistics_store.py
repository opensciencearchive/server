"""Postgres adapter for the instance-statistics snapshot.

Storage size is summed via ``pg_total_relation_size`` over ``records`` plus every
dynamic ``features.*`` and ``metadata.*`` table (enumerated from their catalogs —
never string-built from user input; ``to_regclass`` yields NULL for a missing
table so a dropped table can't error the sum). Feature-row totals are a genuine
O(rows) scan, which is exactly why the result is materialized.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.record.model.statistics import InstanceStats
from osa.infrastructure.persistence.api_naming import (
    feature_pg_schema,
    metadata_pg_schema,
)
from osa.infrastructure.persistence.tables import (
    feature_tables_table,
    instance_statistics_table,
    records_table,
)

# Feature/metadata pg_table names are system-generated and validated on creation
# (``_validate_pg_identifier``); re-check defensively before interpolating.
_SAFE_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class PostgresStatisticsStore:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def count_this_month(self) -> int:
        stmt = (
            select(func.count())
            .select_from(records_table)
            .where(records_table.c.published_at >= func.date_trunc("month", func.now()))
        )
        return int((await self.session.execute(stmt)).scalar_one())

    async def read_snapshot(self) -> InstanceStats | None:
        row = (await self.session.execute(select(instance_statistics_table))).mappings().first()
        if row is None:
            return None
        return InstanceStats(
            storage_bytes=row["storage_bytes"],
            feature_rows=row["feature_rows"],
            computed_at=row["computed_at"],
        )

    async def compute_snapshot(self) -> InstanceStats:
        return InstanceStats(
            storage_bytes=await self._storage_bytes(),
            feature_rows=await self._feature_rows(),
            computed_at=datetime.now(UTC),
        )

    async def refresh(self) -> None:
        snapshot = await self.compute_snapshot()
        # Singleton upsert: clear then insert row id=1.
        await self.session.execute(sa.delete(instance_statistics_table))
        await self.session.execute(
            sa.insert(instance_statistics_table).values(
                id=1,
                storage_bytes=snapshot.storage_bytes,
                feature_rows=snapshot.feature_rows,
                computed_at=snapshot.computed_at,
            )
        )

    async def _storage_bytes(self) -> int:
        stmt = text(
            """
            SELECT
              COALESCE(pg_total_relation_size(to_regclass('records')), 0)
              + COALESCE((
                  SELECT sum(pg_total_relation_size(
                      to_regclass(:fschema || '.' || quote_ident(pg_table))))
                  FROM feature_tables), 0)
              + COALESCE((
                  SELECT sum(pg_total_relation_size(
                      to_regclass(:mschema || '.' || quote_ident(pg_table))))
                  FROM metadata_tables), 0) AS bytes
            """
        )
        result = await self.session.execute(
            stmt, {"fschema": feature_pg_schema(), "mschema": metadata_pg_schema()}
        )
        return int(result.scalar_one() or 0)

    async def _feature_rows(self) -> int:
        names = (
            (await self.session.execute(select(feature_tables_table.c.pg_table))).scalars().all()
        )
        schema = feature_pg_schema()
        total = 0
        for name in names:
            if not _SAFE_IDENT.match(name):
                continue
            stmt = text(f'SELECT count(*) FROM "{schema}"."{name}"')
            total += int((await self.session.execute(stmt)).scalar_one())
        return total
