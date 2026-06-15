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
