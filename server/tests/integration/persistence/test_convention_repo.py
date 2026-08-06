"""Integration tests for ConventionRepository against real PostgreSQL.

Feature #145: conventions are unversioned, slug-keyed (``ConventionSlug``) and
mutable. ``save`` is an idempotent UPSERT by slug (re-saving updates in place,
preserving ``created_at``). Hooks are referenced by **name** (``HookName``) —
the versioned release each name resolves to lives in the validation hook
registry, not inline on the convention.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.hook import HookName
from osa.domain.shared.model.source import (
    IngesterDefinition,
    IngesterLimits,
    IngesterScheduleConfig,
    InitialRunConfig,
)
from osa.domain.shared.model.srn import ConventionSlug, SchemaId
from osa.infrastructure.persistence.repository.convention import (
    PostgresConventionRepository,
)

from tests.factories import make_convention_docs


def _make_convention(
    *,
    slug: str = "test-convention",
    title: str = "Test Convention",
    schema_id: str = "test-schema-001@1.0.0",
    hooks: list[HookName] | None = None,
    ingester: IngesterDefinition | None = None,
) -> Convention:
    return Convention(
        id=ConventionSlug.parse(slug),
        title=title,
        description="A test convention for integration tests",
        schema_id=SchemaId.parse(schema_id),
        file_requirements=FileRequirements(
            accepted_types=[".csv", ".h5ad"],
            min_count=1,
            max_count=5,
            max_file_size=100_000_000,
        ),
        hooks=hooks or [],
        ingester=ingester,
        docs=make_convention_docs(),
        created_at=datetime.now(UTC),
    )


def _make_hook() -> HookName:
    return HookName("quality_check")


def _make_ingester() -> IngesterDefinition:
    return IngesterDefinition(
        image="ghcr.io/example/ingester:latest",
        digest="sha256:def456",
        runner="oci",
        config={"api_key": "test-key"},
        limits=IngesterLimits(timeout_seconds=7200, memory="8g", cpu="4.0"),
        schedule=IngesterScheduleConfig(cron="0 2 * * *", limit=500),
        initial_run=InitialRunConfig(limit=100),
    )


@pytest.mark.asyncio
class TestConventionRepoRoundTrip:
    async def test_save_and_get(self, pg_session: AsyncSession):
        """Save a convention and retrieve it — all fields should match."""
        repo = PostgresConventionRepository(pg_session)
        hook = _make_hook()
        ingester = _make_ingester()
        conv = _make_convention(hooks=[hook], ingester=ingester)

        await repo.save(conv)
        await pg_session.commit()

        got = await repo.get(conv.id)
        assert got is not None
        assert str(got.id) == str(conv.id)
        assert got.title == conv.title
        assert got.description == conv.description
        assert str(got.schema_id) == str(conv.schema_id)
        assert got.file_requirements == conv.file_requirements
        assert len(got.hooks) == 1
        # Hooks are name references now (registry resolves the live release).
        assert got.hooks[0].root == "quality_check"
        assert got.ingester is not None
        assert got.ingester.image == ingester.image
        assert got.ingester.schedule is not None
        assert got.ingester.schedule.cron == "0 2 * * *"
        assert got.ingester.initial_run is not None
        assert got.ingester.initial_run.limit == 100

    async def test_get_nonexistent_returns_none(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        got = await repo.get(ConventionSlug.parse("does-not-exist"))
        assert got is None

    async def test_save_is_idempotent_upsert_preserving_created_at(self, pg_session: AsyncSession):
        """Re-saving a slug updates in place and preserves the original created_at
        (conventions are mutable + unversioned in #145 — no insert conflict)."""
        repo = PostgresConventionRepository(pg_session)
        conv = _make_convention(slug="upsert-me", title="Original")
        original_created_at = conv.created_at

        await repo.save(conv)
        await pg_session.commit()

        # Re-declare with a different title + later created_at; upsert keeps the
        # original created_at but applies the new title.
        updated = _make_convention(slug="upsert-me", title="Updated")
        await repo.save(updated)
        await pg_session.commit()

        got = await repo.get(ConventionSlug.parse("upsert-me"))
        assert got is not None
        assert got.title == "Updated"
        assert got.created_at == original_created_at

        # Still exactly one row for this slug.
        result = await repo.list()
        assert sum(1 for c in result if c.id.root == "upsert-me") == 1

    async def test_list_returns_ordered_by_created_at_desc(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)

        conv_a = _make_convention(slug="conv-aaa", title="First")
        conv_b = _make_convention(slug="conv-bbb", title="Second")

        await repo.save(conv_a)
        await pg_session.flush()
        await repo.save(conv_b)
        await pg_session.commit()

        result = await repo.list()
        assert len(result) == 2
        # Most recent first
        assert result[0].title == "Second"
        assert result[1].title == "First"

    async def test_list_with_limit_and_offset(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)

        for i in range(5):
            conv = _make_convention(
                slug=f"conv-{i:03d}",
                title=f"Conv {i}",
            )
            await repo.save(conv)
            await pg_session.flush()

        await pg_session.commit()

        page = await repo.list(limit=2, offset=1)
        assert len(page) == 2

    async def test_convention_without_ingester(self, pg_session: AsyncSession):
        """Ingester is optional — should be None on retrieval when not set."""
        repo = PostgresConventionRepository(pg_session)
        conv = _make_convention(ingester=None, hooks=[])
        await repo.save(conv)
        await pg_session.commit()

        got = await repo.get(conv.id)
        assert got is not None
        assert got.ingester is None
        assert got.hooks == []
