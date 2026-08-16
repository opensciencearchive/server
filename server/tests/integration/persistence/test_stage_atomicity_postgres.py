"""Feature DML joins the unit of work (#219 phase 4).

``PostgresFeatureStore.insert_features`` was the only DML in the system running
on a private engine connection — it committed independently of the caller's
session, so a stage could half-land: records rolled back, feature rows durable.
These tests demand the property that could not hold before: feature rows
written in a stage commit and roll back WITH the session.

``create_table`` (DDL) stays engine-scoped — the sanctioned MetadataStore
split (DDL on engine, DML on session).

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.shared.model.hook import ColumnDef
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore

from tests.integration.conftest import seed_hook_run, seed_record

HOOK = "atomic_features"
SRN = "urn:osa:localhost:rec:atomic1@1"


def _columns() -> list[ColumnDef]:
    return [ColumnDef(name="score", json_type="number", required=True)]


async def _count_rows(engine: AsyncEngine) -> int:
    async with engine.connect() as conn:
        result = await conn.execute(sa.text(f'SELECT count(*) FROM features."{HOOK}"'))
        return int(result.scalar_one())


async def _setup(engine: AsyncEngine, session: AsyncSession) -> str:
    run_id = await seed_hook_run(engine, feature_name=HOOK, columns=_columns())
    await PostgresFeatureStore(engine, session).create_table(HOOK, _columns())
    await seed_record(
        engine,
        srn=SRN,
        schema_id="compound",
        schema_version="1.0.0",
        metadata={},
        published_at=datetime.now(UTC),
    )
    return run_id


@pytest.mark.asyncio
class TestFeatureDmlJoinsTheUnitOfWork:
    async def test_uncommitted_feature_rows_roll_back_with_the_session(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        run_id = await _setup(pg_engine, pg_session)
        store = PostgresFeatureStore(pg_engine, pg_session)

        inserted = await store.insert_features(HOOK, SRN, [{"score": 1.0}], run_id)
        assert inserted == 1
        await pg_session.rollback()

        assert await _count_rows(pg_engine) == 0, (
            "feature rows survived a session rollback — insert_features is "
            "committing outside the unit of work"
        )

    async def test_committed_feature_rows_are_durable(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        run_id = await _setup(pg_engine, pg_session)
        store = PostgresFeatureStore(pg_engine, pg_session)

        inserted = await store.insert_features(HOOK, SRN, [{"score": 1.0}, {"score": 2.0}], run_id)
        assert inserted == 2
        await pg_session.commit()
        assert await _count_rows(pg_engine) == 2

    async def test_replace_by_record_stays_in_transaction(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """Redo converges (replace semantics) and the replacement itself is
        transactional: a rollback restores the previous rows."""
        run_id = await _setup(pg_engine, pg_session)
        store = PostgresFeatureStore(pg_engine, pg_session)

        await store.insert_features(HOOK, SRN, [{"score": 1.0}], run_id)
        await pg_session.commit()

        await store.insert_features(HOOK, SRN, [{"score": 9.0}, {"score": 8.0}], run_id)
        await pg_session.rollback()
        assert await _count_rows(pg_engine) == 1, "rollback must restore the replaced rows"

        await store.insert_features(HOOK, SRN, [{"score": 9.0}, {"score": 8.0}], run_id)
        await pg_session.commit()
        assert await _count_rows(pg_engine) == 2
