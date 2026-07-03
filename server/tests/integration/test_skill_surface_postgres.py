"""End-to-end HTTP tests for the skill surface against real Postgres (#151, US1/US4).

Drives ``GET /``, ``GET /SKILL.md``, and ``GET /api/v1/data/{schema}.md``
through the full stack, including the degraded states (zero-schema node,
orphan schema, unknown reference ids).

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

import os
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

from tests.factories import make_convention_docs_dict
from tests.integration.conftest import seed_record

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-integration-tests-minimum-32-chars")

if "postgresql" not in os.environ.get("OSA_DATABASE__URL", ""):
    pytest.skip("OSA_DATABASE__URL not set to PostgreSQL", allow_module_level=True)

SCHEMA = SchemaId.parse("compound@1.0.0")


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _seed_documented_schema(engine: AsyncEngine, session: AsyncSession) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=SCHEMA, title="Compounds", fields=_fields(), created_at=datetime.now(UTC))
    )
    await session.execute(
        conventions_table.insert().values(
            id="compound-conv",
            title="Compound Conv",
            description="compound convention",
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            file_requirements={},
            hooks=[],
            source=None,
            docs=make_convention_docs_dict(purpose="Compound reference data."),
            created_at=datetime.now(UTC),
        )
    )
    srn = RecordSRN.parse("urn:osa:localhost:rec:rec1@1")
    await seed_record(
        engine,
        srn=str(srn),
        schema_id=SCHEMA.id.root,
        schema_version=SCHEMA.version.root,
        metadata={"species": "Homo sapiens"},
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    await store.insert(SCHEMA, srn, {"species": "Homo sapiens"})
    await session.commit()


@pytest.fixture
def client() -> AsyncClient:
    from osa.application.api.rest.app import create_app

    return AsyncClient(transport=ASGITransport(app=create_app()), base_url="http://test")


@pytest.mark.asyncio
class TestRootDiscovery:
    async def test_shape(self, pg_engine, pg_session, client: AsyncClient):
        await _seed_documented_schema(pg_engine, pg_session)
        async with client:
            resp = await client.get("/")
        assert resp.status_code == 200
        body = resp.json()
        assert body["node"]["name"] == "Open Science Archive"
        assert body["node"]["domain"] == "localhost"
        assert body["node"]["osa_version"]
        assert body["skill_url"] == "http://localhost:8000/SKILL.md"
        assert body["reference_base"] == "http://localhost:8000/api/v1/data/"
        assert body["data_url"] == "http://localhost:8000/api/v1/data"
        # openapi_url reflects the app's ACTUAL OpenAPI path (default /openapi.json).
        assert body["openapi_url"] == "http://localhost:8000/openapi.json"
        assert body["schemas"] == ["compound@1.0.0"]

    async def test_zero_schemas_never_404s(self, pg_session, client: AsyncClient):
        async with client:
            resp = await client.get("/")
        assert resp.status_code == 200
        assert resp.json()["schemas"] == []


@pytest.mark.asyncio
class TestSkillDocument:
    async def test_skill_md(self, pg_engine, pg_session, client: AsyncClient):
        await _seed_documented_schema(pg_engine, pg_session)
        async with client:
            resp = await client.get("/SKILL.md")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/markdown")
        body = resp.text
        assert body.startswith("---\nname: osa-data-localhost\n")
        assert "| compound@1.0.0 | Compounds | 1 | api/v1/data/compound@1.0.0.md |" in body
        assert "Compound reference data." in body

    async def test_zero_schema_skill_md(self, pg_session, client: AsyncClient):
        async with client:
            resp = await client.get("/SKILL.md")
        assert resp.status_code == 200
        assert "No datasets yet." in resp.text


@pytest.mark.asyncio
class TestSchemaReference:
    async def test_reference_markdown_and_manifest_json_coexist(
        self, pg_engine, pg_session, client: AsyncClient
    ):
        await _seed_documented_schema(pg_engine, pg_session)
        async with client:
            ref = await client.get("/api/v1/data/compound.md")
            manifest = await client.get("/api/v1/data/compound")
        assert ref.status_code == 200
        assert ref.headers["content-type"].startswith("text/markdown")
        assert ref.text.startswith("# Compounds (compound@1.0.0)")
        assert "Compound reference data." in ref.text
        # Sampled value reaches the FilterExpr example (research §9).
        assert '"value": "Homo sapiens"' in ref.text
        # Example URLs are pinned to the documented version — a bare id would
        # silently resolve to latest and drift from the doc being read.
        assert "/api/v1/data/compound@1.0.0/records.csv.gz" in ref.text
        # Route-ordering regression: the JSON manifest still resolves.
        assert manifest.status_code == 200
        assert manifest.json()["id"] == "compound"

    async def test_orphan_schema_renders_no_docs_note(
        self, pg_engine, pg_session, client: AsyncClient
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA, _fields())
        await PostgresSemanticsSchemaRepository(pg_session).save(
            Schema(id=SCHEMA, title="Compounds", fields=_fields(), created_at=datetime.now(UTC))
        )
        await pg_session.commit()
        async with client:
            resp = await client.get("/api/v1/data/compound.md")
        assert resp.status_code == 200
        assert "> No author documentation exists for this dataset." in resp.text
        assert "## Worked examples" not in resp.text

    async def test_unknown_schema_404(self, pg_session, client: AsyncClient):
        async with client:
            resp = await client.get("/api/v1/data/unknown.md")
        assert resp.status_code == 404
