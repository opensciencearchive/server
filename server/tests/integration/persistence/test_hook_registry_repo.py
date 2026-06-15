"""Integration tests for the Postgres hook registry against real PostgreSQL.

Focus: ``upsert_identity`` must be race-safe (#145). Two concurrent deploys of
the same brand-new hook name used to both read an empty result and both INSERT,
the loser hitting the ``hooks`` primary-key constraint (→ 500). The ON CONFLICT
DO NOTHING path makes the loser a no-op instead.
"""

import asyncio

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from osa.domain.shared.model.hook import ColumnDef, HookName, TableFeatureSpec
from osa.infrastructure.persistence.repository.hook_registry import PostgresHookRegistry
from osa.infrastructure.persistence.tables import hooks_table


def _feature() -> TableFeatureSpec:
    return TableFeatureSpec(
        cardinality="many",
        columns=[ColumnDef(name="score", json_type="number", required=True)],
    )


@pytest.mark.asyncio
async def test_concurrent_upsert_identity_same_new_name_is_race_safe(
    pg_engine: AsyncEngine,
    pg_session: AsyncSession,  # ensures TRUNCATE cleanup around this test
) -> None:
    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    name = HookName("concurrent_race_hook")
    feature = _feature()

    async def worker() -> HookName:
        # Each worker owns its own session/transaction so the two upserts race.
        async with factory() as session:
            registry = PostgresHookRegistry(session)
            hook = await registry.upsert_identity(name, feature)
            await session.commit()
            return hook.name

    # Both must succeed (no PK violation) and resolve to the same hook.
    results = await asyncio.gather(worker(), worker())
    assert results == [name, name]

    # Exactly one row exists — the loser no-opped, it didn't duplicate or error.
    async with factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(hooks_table).where(hooks_table.c.name == name.root)
        )
    assert count == 1


@pytest.mark.asyncio
async def test_upsert_identity_rejects_differing_contract(
    pg_engine: AsyncEngine,
    pg_session: AsyncSession,
) -> None:
    from osa.domain.shared.error import ConflictError

    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    name = HookName("contract_hook")

    async with factory() as session:
        await PostgresHookRegistry(session).upsert_identity(name, _feature())
        await session.commit()

    # Same name, different columns → the fixed-contract guard still fires.
    other = TableFeatureSpec(
        cardinality="one",
        columns=[ColumnDef(name="label", json_type="string", required=True)],
    )
    async with factory() as session:
        with pytest.raises(ConflictError):
            await PostgresHookRegistry(session).upsert_identity(name, other)


@pytest.mark.asyncio
async def test_record_run_is_idempotent_on_id(
    pg_engine: AsyncEngine,
    pg_session: AsyncSession,
) -> None:
    """record_run with the same (deterministic) id twice → one row, not a duplicate.

    This is the DB half of idempotent batch retry (#145): the handler derives a
    deterministic hook_run id from (ingest_run_id, batch_index, hook_name), so a
    worker retry / duplicate delivery re-records the same id; ON CONFLICT DO
    NOTHING keeps provenance append-once.
    """
    from datetime import UTC, datetime
    from uuid import uuid4

    from osa.domain.shared.model.hook import OciConfig, OciLimits
    from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
    from osa.infrastructure.persistence.tables import hook_runs_table

    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    name = HookName("idem_run_hook")
    async with factory() as session:
        reg = PostgresHookRegistry(session)
        await reg.upsert_identity(name, _feature())
        outcome = await reg.create_release(
            name,
            OciConfig(image="reg/x:1", digest="sha256:idem", limits=OciLimits()),
            source_ref="git:1",
            built_by=None,
        )
        await session.commit()
        release_id = outcome.release.id

    run_id = HookRunId(uuid4())
    now = datetime.now(UTC)

    def _run() -> HookRun:
        return HookRun(
            id=run_id,
            release_id=release_id,
            status=HookRunStatus.PASSED,
            started_at=now,
            finished_at=now,
            duration_s=1.0,
            oom_retries=0,
        )

    # Record the SAME id twice — second is a worker-retry / duplicate-delivery no-op.
    for _ in range(2):
        async with factory() as session:
            await PostgresHookRegistry(session).record_run(_run())
            await session.commit()

    async with factory() as session:
        count = await session.scalar(
            select(func.count()).select_from(hook_runs_table).where(hook_runs_table.c.id == run_id)
        )
    assert count == 1


@pytest.mark.asyncio
async def test_record_run_persists_log_ref(
    pg_engine: AsyncEngine,
    pg_session: AsyncSession,
) -> None:
    """An ERROR run's log_ref (failed-container log locator) round-trips (#145/#147)."""
    from datetime import UTC, datetime
    from uuid import uuid4

    from osa.domain.shared.model.hook import OciConfig, OciLimits
    from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
    from osa.infrastructure.persistence.tables import hook_runs_table

    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    name = HookName("log_ref_hook")
    async with factory() as session:
        reg = PostgresHookRegistry(session)
        await reg.upsert_identity(name, _feature())
        outcome = await reg.create_release(
            name,
            OciConfig(image="reg/x:1", digest="sha256:logref", limits=OciLimits()),
            source_ref="git:1",
            built_by=None,
        )
        await session.commit()
        release_id = outcome.release.id

    run_id = HookRunId(uuid4())
    now = datetime.now(UTC)
    log_ref = "/data/runs/abc/hooks/log_ref_hook/output/hook.log"
    async with factory() as session:
        await PostgresHookRegistry(session).record_run(
            HookRun(
                id=run_id,
                release_id=release_id,
                status=HookRunStatus.ERROR,
                started_at=now,
                finished_at=now,
                duration_s=1.0,
                oom_retries=0,
                log_ref=log_ref,
            )
        )
        await session.commit()

    async with factory() as session:
        stored = await session.scalar(
            select(hook_runs_table.c.log_ref).where(hook_runs_table.c.id == run_id)
        )
    assert stored == log_ref
