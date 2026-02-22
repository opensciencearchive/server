"""Integration tests for ConventionRepository against real PostgreSQL."""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookLimits,
    HookManifest,
)
from osa.domain.shared.model.source import (
    InitialRunConfig,
    SourceDefinition,
    SourceLimits,
    SourceScheduleConfig,
)
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.infrastructure.persistence.repository.convention import (
    PostgresConventionRepository,
)


def _make_convention(
    *,
    srn: str = "urn:osa:localhost:conv:test-convention-001@1.0.0",
    title: str = "Test Convention",
    schema_srn: str = "urn:osa:localhost:schema:test-schema-001@1.0.0",
    hooks: list[HookDefinition] | None = None,
    source: SourceDefinition | None = None,
) -> Convention:
    return Convention(
        srn=ConventionSRN.parse(srn),
        title=title,
        description="A test convention for integration tests",
        schema_srn=SchemaSRN.parse(schema_srn),
        file_requirements=FileRequirements(
            accepted_types=[".csv", ".h5ad"],
            min_count=1,
            max_count=5,
            max_file_size=100_000_000,
        ),
        hooks=hooks or [],
        source=source,
        created_at=datetime.now(UTC),
    )


def _make_hook() -> HookDefinition:
    return HookDefinition(
        image="ghcr.io/example/validator:latest",
        digest="sha256:abc123",
        runner="oci",
        config={"threshold": 0.95},
        limits=HookLimits(timeout_seconds=600, memory="4g", cpu="2.0"),
        manifest=HookManifest(
            name="quality-check",
            record_schema="urn:osa:localhost:schema:test-schema-001@1.0.0",
            cardinality="many",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="score", json_type="number", required=True),
                    ColumnDef(name="labels", json_type="array", required=False),
                ]
            ),
        ),
    )


def _make_source() -> SourceDefinition:
    return SourceDefinition(
        image="ghcr.io/example/source:latest",
        digest="sha256:def456",
        runner="oci",
        config={"api_key": "test-key"},
        limits=SourceLimits(timeout_seconds=7200, memory="8g", cpu="4.0"),
        schedule=SourceScheduleConfig(cron="0 2 * * *", limit=500),
        initial_run=InitialRunConfig(limit=100),
    )


@pytest.mark.asyncio
class TestConventionRepoRoundTrip:
    async def test_save_and_get(self, pg_session: AsyncSession):
        """Save a convention and retrieve it — all fields should match."""
        repo = PostgresConventionRepository(pg_session)
        hook = _make_hook()
        source = _make_source()
        conv = _make_convention(hooks=[hook], source=source)

        await repo.save(conv)
        await pg_session.commit()

        got = await repo.get(conv.srn)
        assert got is not None
        assert str(got.srn) == str(conv.srn)
        assert got.title == conv.title
        assert got.description == conv.description
        assert str(got.schema_srn) == str(conv.schema_srn)
        assert got.file_requirements == conv.file_requirements
        assert len(got.hooks) == 1
        assert got.hooks[0].image == hook.image
        assert got.hooks[0].digest == hook.digest
        assert got.hooks[0].manifest.name == "quality-check"
        assert got.hooks[0].manifest.feature_schema.columns[0].name == "score"
        assert got.source is not None
        assert got.source.image == source.image
        assert got.source.schedule is not None
        assert got.source.schedule.cron == "0 2 * * *"
        assert got.source.initial_run is not None
        assert got.source.initial_run.limit == 100

    async def test_get_nonexistent_returns_none(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        got = await repo.get(ConventionSRN.parse("urn:osa:localhost:conv:does-not-exist@1.0.0"))
        assert got is None

    async def test_list_returns_ordered_by_created_at_desc(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)

        conv_a = _make_convention(srn="urn:osa:localhost:conv:conv-aaa@1.0.0", title="First")
        conv_b = _make_convention(srn="urn:osa:localhost:conv:conv-bbb@1.0.0", title="Second")

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
                srn=f"urn:osa:localhost:conv:conv-{i:03d}@1.0.0",
                title=f"Conv {i}",
            )
            await repo.save(conv)
            await pg_session.flush()

        await pg_session.commit()

        page = await repo.list(limit=2, offset=1)
        assert len(page) == 2

    async def test_exists_true(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        conv = _make_convention()
        await repo.save(conv)
        await pg_session.commit()

        assert await repo.exists(conv.srn) is True

    async def test_exists_false(self, pg_session: AsyncSession):
        repo = PostgresConventionRepository(pg_session)
        assert await repo.exists(ConventionSRN.parse("urn:osa:localhost:conv:nope@1.0.0")) is False

    async def test_convention_without_source(self, pg_session: AsyncSession):
        """Source is optional — should be None on retrieval when not set."""
        repo = PostgresConventionRepository(pg_session)
        conv = _make_convention(source=None, hooks=[])
        await repo.save(conv)
        await pg_session.commit()

        got = await repo.get(conv.srn)
        assert got is not None
        assert got.source is None
        assert got.hooks == []
