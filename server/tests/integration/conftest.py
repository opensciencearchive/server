"""Fixtures for PostgreSQL integration tests."""

import json
import os
from datetime import UTC, datetime
from typing import Any

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


async def seed_record(
    engine: AsyncEngine,
    *,
    srn: str,
    convention_srn: str = "urn:osa:localhost:conv:test@1.0.0",
    schema_id: str = "test",
    schema_version: str = "1.0.0",
    source: dict[str, Any] | None = None,
    metadata: dict[str, Any] | None = None,
    published_at: datetime | None = None,
) -> None:
    """Insert a records row directly so typed-table FK inserts succeed.

    Keeps tests independent of the full publish event chain when they only
    need a persisted Record to anchor metadata/feature rows against.
    """
    src = source or {"type": "deposition", "id": f"dep-{srn.split(':')[-1]}"}
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                INSERT INTO records (srn, convention_srn, schema_id, schema_version,
                                     source, metadata, published_at)
                VALUES (:srn, :conv, :schema_id, :schema_version,
                        CAST(:source AS JSONB), CAST(:meta AS JSONB), :published_at)
                """
            ),
            {
                "srn": srn,
                "conv": convention_srn,
                "schema_id": schema_id,
                "schema_version": schema_version,
                "source": json.dumps(src),
                "meta": json.dumps(metadata or {}),
                "published_at": published_at or datetime.now(UTC),
            },
        )


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

    # Truncate static tables + drop the two schemas that hold runtime-created
    # dynamic tables (features.<hook>, metadata.<slug>_v<major>). Without the
    # drop, a dynamic table created by test A survives TRUNCATE and collides
    # when test B tries to ensure/create it again.
    async with pg_engine.begin() as conn:
        await conn.execute(
            text(
                "TRUNCATE TABLE depositions, conventions, schemas, ontologies, "
                "ontology_terms, events, deliveries, records, validation_runs, "
                "feature_tables, metadata_tables, users, identities, refresh_tokens, "
                "role_assignments CASCADE"
            )
        )
        await conn.execute(text('DROP SCHEMA IF EXISTS "features" CASCADE'))
        await conn.execute(text('DROP SCHEMA IF EXISTS "metadata" CASCADE'))

    # Re-seed system user after truncate
    await ensure_system_user(pg_engine)
