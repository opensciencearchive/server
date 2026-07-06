"""Integration tests for the skill read-store projections against real Postgres (#151, US1).

``get_author_docs`` projects ``conventions.docs`` by schema id (latest deploy
wins; ``None`` only when no owning convention exists). ``sample_value``
returns one non-null value per research §9 (identifier-quoted, schema-scoped)
and ``None`` on an empty column.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ConflictError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import Domain, RecordSRN, SchemaId
from osa.infrastructure.data.postgres_catalog_read_store import PostgresCatalogReadStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

from tests.factories import make_convention_docs_dict
from tests.integration.conftest import seed_hook_run, seed_record

os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-integration-tests-minimum-32-chars")

if "postgresql" not in os.environ.get("OSA_DATABASE__URL", ""):
    pytest.skip("OSA_DATABASE__URL not set to PostgreSQL", allow_module_level=True)

SCHEMA = SchemaId.parse("compound@1.0.0")
HOOK = "chem_features"


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="notes",
            type=FieldType.TEXT,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _setup_schema(engine: AsyncEngine, session: AsyncSession) -> PostgresMetadataStore:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    return store


async def _insert_convention(
    session: AsyncSession,
    *,
    slug: str,
    purpose: str,
    created_at: datetime,
    hooks: list[str] | None = None,
) -> None:
    await session.execute(
        conventions_table.insert().values(
            id=slug,
            title=slug,
            description="a convention",
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            file_requirements={},
            hooks=hooks or [],
            source=None,
            docs=make_convention_docs_dict(purpose=purpose),
            created_at=created_at,
        )
    )
    await session.commit()


@pytest.mark.asyncio
class TestGetAuthorDocs:
    async def test_projects_convention_docs(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _setup_schema(pg_engine, pg_session)
        await _insert_convention(
            pg_session, slug="compound-conv", purpose="Compound data.", created_at=datetime.now(UTC)
        )
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        docs = await rs.get_author_docs(SCHEMA)
        assert docs is not None
        assert docs.purpose == "Compound data."
        assert len(docs.examples) == 1

    async def test_latest_deploy_wins(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _setup_schema(pg_engine, pg_session)
        now = datetime.now(UTC)
        await _insert_convention(
            pg_session,
            slug="older-conv",
            purpose="Old purpose.",
            created_at=now - timedelta(days=1),
        )
        await _insert_convention(
            pg_session, slug="newer-conv", purpose="New purpose.", created_at=now
        )
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        docs = await rs.get_author_docs(SCHEMA)
        assert docs is not None
        assert docs.purpose == "New purpose."

    async def test_none_when_no_owning_convention(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        assert await rs.get_author_docs(SCHEMA) is None


@pytest.mark.asyncio
class TestSampleValue:
    async def _publish(self, engine, store, rid: str, metadata: dict) -> RecordSRN:
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
        await seed_record(
            engine,
            srn=str(srn),
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            metadata=metadata,
            published_at=datetime(2026, 1, 1, tzinfo=UTC),
        )
        await store.insert(SCHEMA, srn, metadata)
        return srn

    async def test_samples_records_column(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = await _setup_schema(pg_engine, pg_session)
        await self._publish(pg_engine, store, "rec1", {"species": "Homo sapiens"})
        await pg_session.commit()
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        sample = await rs.sample_value(SCHEMA, "records", "species")
        assert sample is not None
        assert sample.value == "Homo sapiens"

    async def test_none_on_empty_column(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = await _setup_schema(pg_engine, pg_session)
        await self._publish(pg_engine, store, "rec1", {"species": "Homo sapiens"})
        await pg_session.commit()
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        assert await rs.sample_value(SCHEMA, "records", "notes") is None

    async def test_none_on_empty_table(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        assert await rs.sample_value(SCHEMA, "records", "species") is None

    async def test_samples_feature_table_column(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        srn = await self._publish(pg_engine, store, "rec1", {"species": "Homo sapiens"})
        # The schema → feature link goes through a convention's hooks list.
        await _insert_convention(
            pg_session,
            slug="compound-conv",
            purpose="Compound data.",
            created_at=datetime.now(UTC),
            hooks=[HOOK],
        )
        columns = [ColumnDef(name="score", json_type="number", required=True)]
        run_id = await seed_hook_run(pg_engine, feature_name=HOOK, columns=columns)
        feature_store = PostgresFeatureStore(pg_engine, pg_session)
        try:
            await feature_store.create_table(HOOK, columns)
        except ConflictError:
            pass
        await feature_store.insert_features(HOOK, str(srn), [{"score": 0.9}], run_id)

        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        sample = await rs.sample_value(SCHEMA, HOOK, "score")
        assert sample is not None
        assert sample.value == 0.9
