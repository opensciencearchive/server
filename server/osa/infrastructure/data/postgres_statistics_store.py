"""Postgres adapter for instance statistics + table_statistics verification.

Storage size is the one fact only observable by polling the storage engine —
``pg_total_relation_size`` over ``records`` plus every dynamic ``features.*``
and ``metadata.*`` table (enumerated from their catalogs — never string-built
from user input; ``to_regclass`` yields NULL for a missing table so a dropped
table can't error the sum). Everything countable comes from the
lockstep-maintained ``table_statistics`` (#219): the snapshot SUMs stored
counts instead of sweeping tables with COUNT(*).

The verifier methods (:meth:`table_statistics_drift` / :meth:`repair_table_statistics`)
hold the truth query — the same group-bys the backfill migration ran — and are
the only sanctioned whole-table counting after deploy.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.data.model.statistics import (
    FeatureCount,
    InstanceStats,
    RecordsCount,
    StatisticsDrift,
    TableCountEntry,
)
from osa.infrastructure.persistence.api_naming import (
    feature_pg_schema,
    metadata_pg_schema,
)
from osa.infrastructure.persistence.tables import (
    instance_statistics_table,
    records_table,
    table_statistics_table,
)

# Feature/metadata pg_table names are system-generated and validated on creation
# (``_validate_pg_identifier``); re-check defensively before interpolating.
_SAFE_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


class PostgresStatisticsStore:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def count_this_month(self) -> int:
        # Live but bounded: index-served month window (idx_records_published_at).
        stmt = (
            select(func.count())
            .select_from(records_table)
            .where(records_table.c.published_at >= func.date_trunc("month", func.now()))
        )
        return int((await self.session.execute(stmt)).scalar_one())

    async def records_total(self) -> int:
        stmt = select(func.coalesce(func.sum(table_statistics_table.c.row_count), 0)).where(
            table_statistics_table.c.table_name == "records"
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
        """Total feature rows = SUM over the lockstep counts — no table sweep."""
        stmt = select(func.coalesce(func.sum(table_statistics_table.c.row_count), 0)).where(
            table_statistics_table.c.table_name != "records"
        )
        return int((await self.session.execute(stmt)).scalar_one())

    # ------------------------------------------------------------------ #
    # Verifier: recompute truth, diff, repair (#219 phase 6)
    # ------------------------------------------------------------------ #

    async def _lock_statistics(self) -> None:
        """Serialize the verifier against every counted write (PR #220 review).

        Taken BEFORE the truth read, held to commit. Lockstep is what makes one
        lock sufficient: every write to a counted table bumps
        ``table_statistics`` in its own transaction, so an in-flight writer
        blocks here while its data rows are still uncommitted (correctly absent
        from our truth) and re-applies its additive delta on the repaired base
        after we commit. Without this, a write landing between truth read and
        delete+reinsert is clobbered — and being additive, the base stays wrong
        forever, not just until the next repair. EXCLUSIVE blocks writers only;
        manifest reads proceed.
        """
        await self.session.execute(text("LOCK TABLE table_statistics IN EXCLUSIVE MODE"))

    async def table_statistics_drift(self) -> list[StatisticsDrift]:
        await self._lock_statistics()
        truth = {_key(e): e for e in await self._recompute_truth()}
        stored = {_key(e): e for e in await self._read_stored()}
        drift: list[StatisticsDrift] = []
        for key in sorted(truth.keys() | stored.keys()):
            t, s = truth.get(key), stored.get(key)
            if (s.counts if s else None) != (t.counts if t else None):
                drift.append(
                    StatisticsDrift(
                        schema_id=key[0],
                        schema_version=key[1],
                        table_name=key[2],
                        stored=s.counts if s else None,
                        actual=t.counts if t else None,
                    )
                )
        return drift

    async def repair_table_statistics(self) -> None:
        """Replace stored counts wholesale with recomputed truth, in one tx."""
        await self._lock_statistics()
        truth = await self._recompute_truth()
        await self.session.execute(sa.delete(table_statistics_table))
        if truth:
            await self.session.execute(
                sa.insert(table_statistics_table),
                [
                    {
                        "schema_id": e.schema_id,
                        "schema_version": e.schema_version,
                        "table_name": e.table_name,
                        "row_count": e.counts.row_count,
                        "records_covered": (
                            e.counts.records_covered if isinstance(e.counts, FeatureCount) else None
                        ),
                        "updated_at": datetime.now(UTC),
                    }
                    for e in truth
                ],
            )

    async def _read_stored(self) -> list[TableCountEntry]:
        result = await self.session.execute(select(table_statistics_table))
        return [
            TableCountEntry(
                schema_id=row["schema_id"],
                schema_version=row["schema_version"],
                table_name=row["table_name"],
                counts=(
                    RecordsCount(row_count=row["row_count"])
                    if row["table_name"] == "records"
                    else FeatureCount(
                        row_count=row["row_count"],
                        records_covered=row["records_covered"] or 0,
                    )
                ),
            )
            for row in result.mappings()
        ]

    async def _recompute_truth(self) -> list[TableCountEntry]:
        """The backfill migration's truth query, kept runnable (#219)."""
        entries: list[TableCountEntry] = []
        records = await self.session.execute(
            text(
                """
                SELECT schema_id, schema_version, count(*) AS n
                FROM records GROUP BY schema_id, schema_version
                """
            )
        )
        for schema_id, schema_version, n in records.fetchall():
            entries.append(
                TableCountEntry(
                    schema_id=schema_id,
                    schema_version=schema_version,
                    table_name="records",
                    counts=RecordsCount(row_count=n),
                )
            )
        tables = await self.session.execute(text("SELECT hook_name, pg_table FROM feature_tables"))
        fschema = feature_pg_schema()
        for hook_name, pg_table in tables.fetchall():
            if not _SAFE_IDENT.match(pg_table):
                continue
            result = await self.session.execute(
                text(
                    f"""
                    SELECT r.schema_id, r.schema_version, count(ft.id) AS n,
                           count(DISTINCT ft.record_srn) AS covered
                    FROM "{fschema}"."{pg_table}" ft
                    JOIN records r ON r.srn = ft.record_srn
                    GROUP BY r.schema_id, r.schema_version
                    """
                )
            )
            for schema_id, schema_version, n, covered in result.fetchall():
                entries.append(
                    TableCountEntry(
                        schema_id=schema_id,
                        schema_version=schema_version,
                        table_name=hook_name,
                        counts=FeatureCount(row_count=n, records_covered=covered),
                    )
                )
        return entries


def _key(e: TableCountEntry) -> tuple[str, str, str]:
    return (e.schema_id, e.schema_version, e.table_name)
