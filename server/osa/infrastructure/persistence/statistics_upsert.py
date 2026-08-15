"""Lockstep ``table_statistics`` upsert (#219 phase 5).

Counts are write-model derived state: the delta is computable from the write
itself, so the writing adapter upserts it inside its own transaction and the
displayed counts always equal committed data. Additive deltas compose under
concurrency — the ON CONFLICT row lock serializes increments, so no update is
lost. Call this ONLY from the adapter that performed the counted DML, on the
same session, before its transaction commits.

The truth query (recompute-from-source) lives with the backfill migration and
the admin verifier — reconciliation is mandatory there, never load-bearing
here.
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from osa.infrastructure.persistence.tables import table_statistics_table


async def bump_table_statistics(
    session: AsyncSession,
    *,
    schema_id: str,
    schema_version: str,
    table_name: str,
    row_delta: int,
    coverage_delta: int | None = None,
) -> None:
    """Add *row_delta* (and optionally *coverage_delta*) to one table's counts.

    ``coverage_delta=None`` is the records-table shape: ``records_covered``
    stays NULL and is never touched. Feature tables pass an int (0 or 1 per
    replaced record batch).
    """
    if row_delta == 0 and not coverage_delta:
        return
    t = table_statistics_table
    now = datetime.now(UTC)
    stmt = insert(t).values(
        schema_id=schema_id,
        schema_version=schema_version,
        table_name=table_name,
        row_count=row_delta,
        records_covered=coverage_delta,
        updated_at=now,
    )
    set_: dict = {
        "row_count": t.c.row_count + row_delta,
        "updated_at": now,
    }
    if coverage_delta is not None:
        set_["records_covered"] = func.coalesce(t.c.records_covered, 0) + coverage_delta
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=["schema_id", "schema_version", "table_name"],
            set_=set_,
        )
    )
