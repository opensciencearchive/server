"""Fixtures for PostgreSQL integration tests."""

import os

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from osa.infrastructure.persistence.seed import ensure_system_user


def _get_pg_url() -> str:
    url = os.environ.get("OSA_DATABASE__URL", "")
    if "postgresql" not in url:
        pytest.skip("OSA_DATABASE__URL not set to PostgreSQL")
    return url


@pytest_asyncio.fixture
async def pg_engine():
    """Per-test async engine pointing at osa_test."""
    url = _get_pg_url()
    engine = create_async_engine(url, pool_pre_ping=True)
    await ensure_system_user(engine)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def pg_session(pg_engine: AsyncEngine):
    """Per-test session with TRUNCATE cleanup."""
    factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with factory() as session:
        yield session
        await session.rollback()

    # Truncate all tables after each test
    async with pg_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE depositions, conventions, schemas, ontologies, "
                "ontology_terms, events, records, validation_runs, "
                "feature_tables, users, identities, refresh_tokens, "
                "role_assignments CASCADE"
            )
        )

    # Re-seed system user after truncate
    await ensure_system_user(pg_engine)
