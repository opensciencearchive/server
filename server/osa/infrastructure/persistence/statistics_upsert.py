"""Lockstep ``table_statistics`` upsert (#219 phase 5).

Counts are write-model derived state: the delta is computable from the write
itself, so the writing adapter upserts it inside its own transaction and the
displayed counts always equal committed data. Additive deltas compose under
concurrency — the ON CONFLICT row lock serializes increments, so no update is
lost. Call this ONLY from the adapter that performed the counted DML, on the
same session, before its transaction commits.

The delta is an algebraic type, mirroring the count models: ``RecordsDelta``
carries no coverage (unrepresentable, not merely unused), ``FeatureDelta``
always does. These are write-side persistence constructs, so they live here
with the helper rather than in the domain model.

The truth query (recompute-from-source) lives with the backfill migration and
the admin verifier — reconciliation is mandatory there, never load-bearing
here.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import SchemaId
from osa.infrastructure.persistence.tables import table_statistics_table


class RecordsDelta(BaseModel):
    """Rows added to a schema version's records table."""

    kind: Literal["records"] = "records"
    rows: int

    @property
    def table_name(self) -> str:
        return "records"


class FeatureDelta(BaseModel):
    """Rows and coverage added to one feature table for a schema version.

    ``covered`` is 0 or 1: writes are per-record (replace-by-record), and a
    record enters coverage on its first feature write only. The bound is
    enforced — a batch-level writer (#218) must widen it deliberately, not
    drift into it.
    """

    kind: Literal["feature"] = "feature"
    feature: str
    rows: int
    covered: int = Field(ge=0, le=1)

    @property
    def table_name(self) -> str:
        return self.feature


CountDelta = RecordsDelta | FeatureDelta


async def bump_table_statistics(
    session: AsyncSession,
    *,
    schema: SchemaId,
    delta: CountDelta,
) -> None:
    """Apply *delta* to one table's counts, inside the caller's transaction."""
    if delta.rows == 0 and (isinstance(delta, RecordsDelta) or delta.covered == 0):
        return
    t = table_statistics_table
    now = datetime.now(UTC)
    covered = delta.covered if isinstance(delta, FeatureDelta) else None
    stmt = insert(t).values(
        schema_id=schema.id.root,
        schema_version=schema.version.root,
        table_name=delta.table_name,
        row_count=delta.rows,
        records_covered=covered,
        updated_at=now,
    )
    set_: dict = {
        "row_count": t.c.row_count + delta.rows,
        "updated_at": now,
    }
    if isinstance(delta, FeatureDelta):
        set_["records_covered"] = func.coalesce(t.c.records_covered, 0) + delta.covered
    await session.execute(
        stmt.on_conflict_do_update(
            index_elements=["schema_id", "schema_version", "table_name"],
            set_=set_,
        )
    )
