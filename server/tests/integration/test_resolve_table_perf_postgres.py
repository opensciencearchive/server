"""``resolve_table`` must not build manifests (#219 phase 1).

Every table read resolves its column schema through
``DataCatalogService.resolve_table``. Before #219 that walked the full
``SchemaManifest`` — three aggregate queries per feature table — and kept only
a column list. These tests hold the tripwire (zero ``count(`` statements) and
pin the resolution semantics the manifest path provided incidentally, so the
rebuild underneath cannot change observable behaviour.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.query_plan import TableKind
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.ids import FeatureName
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

SCHEMA = SchemaId.parse("compound@1.0.0")
SCHEMA_V2 = SchemaId.parse("compound@1.2.0")
HOOK = "chem_features"


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _feature_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="score", json_type="number", required=True),
        ColumnDef(name="label", json_type="string", required=False),
    ]


def _service(session: AsyncSession) -> DataCatalogService:
    return DataCatalogService(read_store=PostgresCatalogReadStore(session, Domain("localhost")))


async def _setup_schema(
    engine: AsyncEngine, session: AsyncSession, schema: SchemaId = SCHEMA
) -> PostgresMetadataStore:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(schema, _fields())
    await PostgresSemanticsSchemaRepository(session).save(
        Schema(id=schema, title=schema.id.root, fields=_fields(), created_at=datetime.now(UTC))
    )
    return store


async def _register_hook(
    engine: AsyncEngine, session: AsyncSession, schema: SchemaId = SCHEMA
) -> str:
    await session.execute(
        conventions_table.insert().values(
            id=f"{schema.id.root}-{HOOK}",
            title="conv",
            description="convention",
            schema_id=schema.id.root,
            schema_version=schema.version.root,
            file_requirements={},
            hooks=[HOOK],
            source=None,
            docs=make_convention_docs_dict(),
            created_at=datetime.now(UTC),
        )
    )
    await session.commit()
    run_id = await seed_hook_run(engine, feature_name=HOOK, columns=_feature_columns())
    await PostgresFeatureStore(engine, session).create_table(HOOK, _feature_columns())
    return run_id


async def _seed_rows(engine: AsyncEngine, store: PostgresMetadataStore, n: int = 3) -> None:
    for i in range(n):
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:rec{i}@1")
        await seed_record(
            engine,
            srn=str(srn),
            schema_id=SCHEMA.id.root,
            schema_version=SCHEMA.version.root,
            metadata={"species": f"sp{i}"},
            published_at=datetime(2026, 1, 1 + i, tzinfo=UTC),
        )
        await store.insert(SCHEMA, srn, {"species": f"sp{i}"})


@pytest.mark.asyncio
class TestResolveTableEmitsNoAggregates:
    """The #219 tripwire: table resolution is O(1) catalog lookups, no counting."""

    async def test_records_resolution_emits_zero_count_statements(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _register_hook(pg_engine, pg_session)
        await _seed_rows(pg_engine, store)
        await pg_session.commit()

        captured_sql.clear()
        resolved = await _service(pg_session).resolve_table("compound@1.0.0", TableKind.RECORDS)

        counting = [s for s in captured_sql if "count(" in s.lower()]
        assert counting == [], f"resolve_table issued aggregate queries: {counting}"
        assert resolved.schema_id == SCHEMA
        assert [c.name for c in resolved.columns[:2]] == ["id", "srn"]

    async def test_feature_resolution_emits_zero_count_statements(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, captured_sql: list[str]
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _register_hook(pg_engine, pg_session)
        await _seed_rows(pg_engine, store)
        await pg_session.commit()

        captured_sql.clear()
        resolved = await _service(pg_session).resolve_table(
            "compound@1.0.0", TableKind.FEATURE, FeatureName(HOOK)
        )

        counting = [s for s in captured_sql if "count(" in s.lower()]
        assert counting == [], f"resolve_table issued aggregate queries: {counting}"
        assert resolved.schema_id == SCHEMA
        assert "score" in [c.name for c in resolved.columns]


@pytest.mark.asyncio
class TestResolveTableContract:
    """Semantics the manifest path provided incidentally, pinned through the rebuild."""

    async def test_bare_id_resolves_latest_schema_version(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session, SCHEMA)
        await _setup_schema(pg_engine, pg_session, SCHEMA_V2)
        await pg_session.commit()

        resolved = await _service(pg_session).resolve_table("compound", TableKind.RECORDS)
        assert resolved.schema_id == SCHEMA_V2

    async def test_reserved_names_are_404(self, pg_session: AsyncSession):
        svc = _service(pg_session)
        for reserved in ("records", "datasets"):
            with pytest.raises(NotFoundError):
                await svc.resolve_table(reserved, TableKind.RECORDS)

    async def test_unknown_schema_is_404(self, pg_session: AsyncSession):
        with pytest.raises(NotFoundError):
            await _service(pg_session).resolve_table("nope@1.0.0", TableKind.RECORDS)

    async def test_unknown_feature_on_known_schema_is_404(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        with pytest.raises(NotFoundError):
            await _service(pg_session).resolve_table(
                "compound@1.0.0", TableKind.FEATURE, FeatureName("no_such_hook")
            )

    async def test_feature_matched_by_name_and_kind(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """A hook can never shadow the records slot, nor vice versa."""
        store = await _setup_schema(pg_engine, pg_session)
        await _register_hook(pg_engine, pg_session)
        await _seed_rows(pg_engine, store, n=1)
        await pg_session.commit()
        svc = _service(pg_session)

        records = await svc.resolve_table("compound@1.0.0", TableKind.RECORDS)
        feature = await svc.resolve_table("compound@1.0.0", TableKind.FEATURE, FeatureName(HOOK))
        record_cols = [c.name for c in records.columns]
        feature_cols = [c.name for c in feature.columns]
        assert "species" in record_cols and "score" not in record_cols
        assert "score" in feature_cols and "species" not in feature_cols

    async def test_resolved_columns_match_manifest_columns(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """The rebuilt lookup returns exactly the columns the manifest declares."""
        store = await _setup_schema(pg_engine, pg_session)
        await _register_hook(pg_engine, pg_session)
        await _seed_rows(pg_engine, store, n=1)
        await pg_session.commit()
        svc = _service(pg_session)

        manifest = await svc.get_schema_manifest(SCHEMA)
        by_name = {(tr.name, tr.kind): tr.columns for tr in manifest.table_resources}

        records = await svc.resolve_table("compound@1.0.0", TableKind.RECORDS)
        assert records.columns == by_name[("records", TableKind.RECORDS)]
        feature = await svc.resolve_table("compound@1.0.0", TableKind.FEATURE, FeatureName(HOOK))
        assert feature.columns == by_name[(HOOK, TableKind.FEATURE)]
