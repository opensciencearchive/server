"""``table_statistics`` maintained in lockstep with the writes (#219 phase 5).

Counts are write-model derived state: the writing adapter upserts the delta
inside its own transaction, so displayed counts always equal committed data —
no sweep, no projection lag, no live COUNT(*). The invariants under test:

- rows and stats commit or roll back together (I1);
- redo converges — replaying a batch nets zero delta (I2);
- deltas are ON CONFLICT-aware: only rows actually inserted count;
- feature coverage gains +1 only on a record's first feature write;
- a fresh table simply has no stats row (absent = zero).

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.record.model.aggregate import Record
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.source import IngestSource
from osa.domain.shared.model.srn import ConventionSlug, RecordSRN, SchemaId
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.repository.record import PostgresRecordRepository
from osa.infrastructure.persistence.tables import table_statistics_table

from tests.integration.conftest import seed_hook_run

SCHEMA = SchemaId.parse("compound@1.0.0")
HOOK = "stats_features"


def _record(i: int) -> Record:
    return Record(
        srn=RecordSRN.parse(f"urn:osa:localhost:rec:stat{i}@1"),
        source=IngestSource(
            id=f"stat-{i}", ingest_run_id="run-1", upstream_source=f"up-{i}", batch_index=0
        ),
        convention_id=ConventionSlug.parse("stats-conv"),
        schema_id=SCHEMA,
        metadata={},
        published_at=datetime(2026, 1, 1, 12, i, tzinfo=UTC),
    )


async def _stats(engine: AsyncEngine, table: str) -> tuple[int, int | None] | None:
    async with engine.connect() as conn:
        result = await conn.execute(
            sa.select(
                table_statistics_table.c.row_count,
                table_statistics_table.c.records_covered,
            ).where(
                table_statistics_table.c.schema_id == SCHEMA.id.root,
                table_statistics_table.c.schema_version == SCHEMA.version.root,
                table_statistics_table.c.table_name == table,
            )
        )
        row = result.first()
    return (row[0], row[1]) if row is not None else None


@pytest.mark.asyncio
class TestRecordCountsLockstep:
    async def test_save_many_bumps_by_rows_actually_inserted(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        repo = PostgresRecordRepository(pg_session)
        inserted = await repo.save_many([_record(1), _record(2), _record(3)])
        assert len(inserted) == 3
        await pg_session.commit()
        assert await _stats(pg_engine, "records") == (3, None)

        # Redo the same batch: ON CONFLICT skips all three — delta must be 0,
        # not 3 (the delta is rows actually inserted, not rows attempted).
        again = await repo.save_many([_record(1), _record(2), _record(3)])
        assert again == []
        await pg_session.commit()
        assert await _stats(pg_engine, "records") == (3, None)

    async def test_rows_and_stats_roll_back_together(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        repo = PostgresRecordRepository(pg_session)
        await repo.save_many([_record(1)])
        await pg_session.commit()

        await repo.save_many([_record(2), _record(3)])
        await pg_session.rollback()
        assert await _stats(pg_engine, "records") == (1, None)

    async def test_single_save_counts_one(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        repo = PostgresRecordRepository(pg_session)
        await repo.save(_record(7))
        await pg_session.commit()
        assert await _stats(pg_engine, "records") == (1, None)

    async def test_fresh_table_has_no_stats_row(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        assert await _stats(pg_engine, "records") is None


@pytest.mark.asyncio
class TestFeatureCountsLockstep:
    async def _setup(self, engine: AsyncEngine, session: AsyncSession) -> str:
        columns = [ColumnDef(name="score", json_type="number", required=True)]
        run_id = await seed_hook_run(engine, feature_name=HOOK, columns=columns)
        await PostgresFeatureStore(engine, session).create_table(HOOK, columns)
        repo = PostgresRecordRepository(session)
        await repo.save_many([_record(1), _record(2)])
        await session.commit()
        return run_id

    async def test_deltas_and_coverage(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        run_id = await self._setup(pg_engine, pg_session)
        store = PostgresFeatureStore(pg_engine, pg_session)
        srn1, srn2 = str(_record(1).srn), str(_record(2).srn)

        await store.insert_features(HOOK, srn1, [{"score": 1.0}, {"score": 2.0}], run_id)
        await pg_session.commit()
        assert await _stats(pg_engine, HOOK) == (2, 1)

        await store.insert_features(
            HOOK, srn2, [{"score": 1.0}, {"score": 2.0}, {"score": 3.0}], run_id
        )
        await pg_session.commit()
        assert await _stats(pg_engine, HOOK) == (5, 2)

    async def test_redo_converges_to_zero_delta(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        run_id = await self._setup(pg_engine, pg_session)
        store = PostgresFeatureStore(pg_engine, pg_session)
        srn1 = str(_record(1).srn)

        await store.insert_features(HOOK, srn1, [{"score": 1.0}, {"score": 2.0}], run_id)
        await pg_session.commit()

        # Redo, same row count: replace-by-record nets 0 rows, 0 coverage.
        await store.insert_features(HOOK, srn1, [{"score": 9.0}, {"score": 8.0}], run_id)
        await pg_session.commit()
        assert await _stats(pg_engine, HOOK) == (2, 1)

        # Redo with MORE rows: delta = inserted - deleted = +2, coverage still 1.
        await store.insert_features(HOOK, srn1, [{"score": float(i)} for i in range(4)], run_id)
        await pg_session.commit()
        assert await _stats(pg_engine, HOOK) == (4, 1)

    async def test_feature_rows_and_stats_roll_back_together(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        run_id = await self._setup(pg_engine, pg_session)
        store = PostgresFeatureStore(pg_engine, pg_session)

        await store.insert_features(HOOK, str(_record(1).srn), [{"score": 1.0}], run_id)
        await pg_session.rollback()
        assert await _stats(pg_engine, HOOK) is None
