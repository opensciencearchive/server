"""Fixtures for PostgreSQL integration tests."""

import json
import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from osa.domain.shared.model.hook import (
    ColumnDef,
    OciConfig,
    TableFeatureSpec,
)
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
from osa.infrastructure.persistence.repository.hook_registry import PostgresHookRegistry
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
    convention_id: str = "urn:osa:localhost:conv:test@1.0.0",
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
                INSERT INTO records (srn, convention_id, schema_id, schema_version,
                                     source, metadata, published_at)
                VALUES (:srn, :conv, :schema_id, :schema_version,
                        CAST(:source AS JSONB), CAST(:meta AS JSONB), :published_at)
                """
            ),
            {
                "srn": srn,
                "conv": convention_id,
                "schema_id": schema_id,
                "schema_version": schema_version,
                "source": json.dumps(src),
                "meta": json.dumps(metadata or {}),
                "published_at": published_at or datetime.now(UTC),
            },
        )


async def seed_hook_run(
    engine: AsyncEngine,
    *,
    feature_name: str,
    columns: list[ColumnDef] | None = None,
) -> str:
    """Seed the per-row provenance chain for a feature table and return a real
    ``hook_runs.id`` (as a string) usable as a feature row's ``run_id``.

    Feature #145 made every ``features.*`` row carry a NOT NULL ``run_id`` that
    is a FK to ``hook_runs.id``. To insert feature rows in a test the chain
    ``hook identity → release → run`` must exist first. Built via the registry
    adapter so the FK is satisfied with real ids.
    """
    if columns is None:
        columns = [
            ColumnDef(name="score", json_type="number", required=True),
            ColumnDef(name="label", json_type="string", required=False),
        ]
    feature = TableFeatureSpec(cardinality="many", columns=columns)
    runtime = OciConfig(image="ghcr.io/example/hook:latest", digest=f"sha256:{uuid4().hex}")

    factory = async_sessionmaker(engine, expire_on_commit=False)
    async with factory() as session:
        from osa.domain.shared.model.hook import HookName

        registry = PostgresHookRegistry(session)
        await registry.upsert_identity(HookName(feature_name), feature)
        release = await registry.create_release(
            HookName(feature_name), runtime, source_ref="abc1234", built_by=None
        )
        now = datetime.now(UTC)
        run = HookRun(
            id=HookRunId(uuid4()),
            release_id=release.id,
            status=HookRunStatus.PASSED,
            started_at=now,
            finished_at=now,
            duration_s=0.1,
            oom_retries=0,
            log_ref=None,
        )
        await registry.record_run(run)
        await session.commit()
        return str(run.id)


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
                "feature_tables, metadata_tables, hooks, hook_releases, hook_runs, "
                "users, identities, refresh_tokens, "
                "role_assignments CASCADE"
            )
        )
        await conn.execute(text('DROP SCHEMA IF EXISTS "features" CASCADE'))
        await conn.execute(text('DROP SCHEMA IF EXISTS "metadata" CASCADE'))
        # Re-create empty ``metadata`` schema. Production relies on the
        # migration having created it; tests need to restore that
        # invariant after the DROP above.
        await conn.execute(text('CREATE SCHEMA "metadata"'))
        await conn.execute(text('CREATE SCHEMA "features"'))

    # Re-seed system user after truncate
    await ensure_system_user(pg_engine)
