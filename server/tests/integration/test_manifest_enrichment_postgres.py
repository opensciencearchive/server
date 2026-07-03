"""Integration tests for the enriched schema manifest against a real Postgres (#151, US3).

Deploy → manifest round-trip: field descriptions/units/examples persisted in
the ``schemas.fields`` JSON blob and column format/description/unit persisted
in the feature-table schema come back through ``get_schema_manifest`` — and
through the HTTP route with absent attributes omitted (FR-019/AS-2).

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

import os
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import (
    Cardinality,
    FieldDefinition,
    FieldType,
    NumberConstraints,
)
from osa.domain.shared.error import ConflictError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import Domain, SchemaId
from osa.infrastructure.data.postgres_catalog_read_store import PostgresCatalogReadStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

from tests.integration.conftest import seed_hook_run

os.environ.setdefault("OSA_BASE_URL", "http://localhost:8000")
os.environ.setdefault("OSA_AUTH__JWT__SECRET", "test-secret-for-integration-tests-minimum-32-chars")

if "postgresql" not in os.environ.get("OSA_DATABASE__URL", ""):
    pytest.skip("OSA_DATABASE__URL not set to PostgreSQL", allow_module_level=True)

SCHEMA = SchemaId.parse("alloy-tests@2.1.0")
HOOK = "ductility"


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="yield_strength",
            type=FieldType.NUMBER,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
            description="0.2% offset yield strength",
            constraints=NumberConstraints(unit="MPa"),
            examples=["512"],
        ),
        FieldDefinition(
            name="alloy",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _feature_columns() -> list[ColumnDef]:
    return [
        ColumnDef(
            name="transition_temp",
            json_type="number",
            format="float",
            required=True,
            description="Ductile-brittle transition",
            unit="°C",
        ),
        ColumnDef(name="batch", json_type="string", required=False),
    ]


async def _seed(engine: AsyncEngine, session: AsyncSession) -> None:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(
            id=SCHEMA,
            title="Alloy Ductility Tests",
            fields=_fields(),
            created_at=datetime.now(UTC),
        )
    )
    await session.execute(
        conventions_table.insert().values(
            id="alloy-tests-conv",
            title="Alloy conv",
            description="Alloy convention",
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            file_requirements={},
            hooks=[HOOK],
            source=None,
            created_at=datetime.now(UTC),
        )
    )
    await session.commit()
    await seed_hook_run(engine, feature_name=HOOK, columns=_feature_columns())
    feature_store = PostgresFeatureStore(engine, session)
    try:
        await feature_store.create_table(HOOK, _feature_columns())
    except ConflictError:
        pass


@pytest.mark.asyncio
class TestManifestEnrichmentReadStore:
    async def test_field_metadata_round_trips(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        manifest = await rs.get_schema_manifest(SCHEMA)
        assert manifest is not None

        assert manifest.title == "Alloy Ductility Tests"

        by_name = {f.name: f for f in manifest.fields}
        enriched = by_name["yield_strength"]
        assert enriched.description == "0.2% offset yield strength"
        assert enriched.unit == "MPa"
        assert enriched.examples == ["512"]

        bare = by_name["alloy"]
        assert bare.description is None
        assert bare.unit is None
        assert bare.examples is None

    async def test_feature_column_metadata_round_trips(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)
        rs = PostgresCatalogReadStore(pg_session, Domain("localhost"))
        manifest = await rs.get_schema_manifest(SCHEMA)
        assert manifest is not None

        feature = next(t for t in manifest.table_resources if t.name == HOOK)
        by_name = {c.name: c for c in feature.columns}
        enriched = by_name["transition_temp"]
        assert enriched.format == "float"
        assert enriched.description == "Ductile-brittle transition"
        assert enriched.unit == "°C"

        bare = by_name["batch"]
        assert bare.format is None
        assert bare.description is None
        assert bare.unit is None


@pytest.mark.asyncio
class TestManifestEnrichmentHttp:
    async def test_manifest_route_serializes_enrichment_and_omits_absent(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _seed(pg_engine, pg_session)

        from osa.application.api.rest.app import create_app

        client = AsyncClient(transport=ASGITransport(app=create_app()), base_url="http://test")
        async with client:
            resp = await client.get("/api/v1/data/alloy-tests@2.1.0")
        assert resp.status_code == 200
        manifest = resp.json()

        assert manifest["title"] == "Alloy Ductility Tests"

        by_name = {f["name"]: f for f in manifest["fields"]}
        enriched = by_name["yield_strength"]
        assert enriched["description"] == "0.2% offset yield strength"
        assert enriched["unit"] == "MPa"
        assert enriched["examples"] == ["512"]

        # AS-2: a field with none of the new attributes carries no new keys —
        # and no null-valued keys at all.
        assert by_name["alloy"] == {"name": "alloy", "type": "text"}

        feature = next(t for t in manifest["table_resources"] if t["name"] == HOOK)
        cols = {c["name"]: c for c in feature["columns"]}
        assert cols["transition_temp"]["format"] == "float"
        assert cols["transition_temp"]["unit"] == "°C"
        assert cols["transition_temp"]["description"] == "Ductile-brittle transition"
        assert cols["batch"] == {"name": "batch", "type": "text"}
