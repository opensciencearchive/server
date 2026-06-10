"""Integration tests for PostgresDataReadStore feature-table streaming (US5).

Exercises the FEATURE branch of the unified ``/data/`` engine against a real
Postgres: a schema with a registered hook produces a ``features.<hook>`` table;
the engine streams it, filters on its columns, and surfaces it in the catalog
and manifest scoped to the owning schema.

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.filter import FilterOperator, FeatureFieldRef, Predicate
from osa.domain.data.model.query_plan import (
    PaginationParams,
    QueryPlan,
    TableKind,
)
from osa.domain.data.model.query_plan import TableKind as TK
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import Domain, RecordSRN, SchemaId
from osa.infrastructure.data.postgres_data_read_store import PostgresDataReadStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)
from osa.infrastructure.persistence.tables import conventions_table

from tests.integration.conftest import seed_record

SCHEMA = SchemaId.parse("compound@1.0.0")
SCHEMA_B = SchemaId.parse("protein@1.0.0")
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
    engine: AsyncEngine,
    session: AsyncSession,
    hook_name: str = HOOK,
    schema: SchemaId = SCHEMA,
) -> None:
    """Link the schema → hook via a convention row, then create its feature table.

    The read store only reads ``hooks[*].name`` from the convention, so a
    name-only hooks payload is sufficient to scope the feature to the schema.
    A pre-existing feature table is reused, mirroring ``CreateFeatureTables``
    (which swallows the ConflictError when two conventions share a hook name).
    """
    await session.execute(
        conventions_table.insert().values(
            srn=f"urn:osa:localhost:conv:{schema.id.root}-{hook_name}@1.0.0",
            title=f"{schema.id.root} conv",
            description=None,
            schema_id=schema.id.root,
            schema_version=schema.version.root,
            file_requirements={},
            hooks=[{"name": hook_name}],
            source=None,
            created_at=datetime.now(UTC),
        )
    )
    await session.commit()
    feature_store = PostgresFeatureStore(engine, session)
    try:
        await feature_store.create_table(hook_name, _feature_columns())
    except ConflictError:
        pass


async def _publish(
    engine: AsyncEngine, store: PostgresMetadataStore, rid: str, schema: SchemaId = SCHEMA
) -> RecordSRN:
    srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
    await seed_record(
        engine,
        srn=str(srn),
        schema_id=schema.id.root,
        schema_version=schema.version.root,
        metadata={"species": "Homo sapiens"},
        published_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    await store.insert(schema, srn, {"species": "Homo sapiens"})
    return srn


def _feature_plan(filter_expr=None, limit=50) -> QueryPlan:
    return QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.FEATURE,
        feature_name=HOOK,
        filter=filter_expr,
        pagination=PaginationParams(limit=limit),
    )


async def _drain(store: PostgresDataReadStore, plan: QueryPlan) -> list[dict]:
    return [dict(row) async for row in store.stream_rows(plan)]


@pytest.mark.asyncio
class TestStreamFeatures:
    async def test_streams_feature_rows_with_data_columns(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        srn = await _publish(pg_engine, store, "rec1")
        await pg_session.commit()
        await _register_hook(pg_engine, pg_session)
        feature_store = PostgresFeatureStore(pg_engine, pg_session)
        await feature_store.insert_features(
            HOOK, str(srn), [{"score": 0.9, "label": "high"}, {"score": 0.1, "label": "low"}]
        )

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        rows = await _drain(rs, _feature_plan())

        assert len(rows) == 2
        # Default FEATURE sort is id asc → first inserted row first.
        first = rows[0]
        assert first["record_srn"] == str(srn)
        assert first["score"] == 0.9
        assert first["label"] == "high"
        assert "id" in first and "created_at" in first

    async def test_feature_filter_narrows_results(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        srn = await _publish(pg_engine, store, "rec1")
        await pg_session.commit()
        await _register_hook(pg_engine, pg_session)
        feature_store = PostgresFeatureStore(pg_engine, pg_session)
        await feature_store.insert_features(
            HOOK, str(srn), [{"score": 0.9, "label": "high"}, {"score": 0.1, "label": "low"}]
        )

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        plan = _feature_plan(
            filter_expr=Predicate(
                field=FeatureFieldRef(hook=HOOK, column="score"),
                op=FilterOperator.GTE,
                value=0.5,
            )
        )
        rows = await _drain(rs, plan)
        assert [r["label"] for r in rows] == ["high"]

    async def test_unknown_feature_raises_not_found(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        plan = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.FEATURE,
            feature_name="nonexistent",
        )
        with pytest.raises(NotFoundError):
            await _drain(rs, plan)


@pytest.mark.asyncio
class TestFeatureSchemaScoping:
    """Two schemas whose conventions register the same hook name share one
    physical ``features.<hook>`` table (``CreateFeatureTables`` swallows the
    ConflictError). Reads at ``/data/{schema}/{feature}`` must still be scoped
    to records of the requested schema."""

    async def _seed_shared_hook(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ) -> tuple[RecordSRN, RecordSRN]:
        store_a = await _setup_schema(pg_engine, pg_session)
        store_b = await _setup_schema(pg_engine, pg_session, schema=SCHEMA_B)
        srn_a = await _publish(pg_engine, store_a, "reca")
        srn_b = await _publish(pg_engine, store_b, "recb", schema=SCHEMA_B)
        await pg_session.commit()
        await _register_hook(pg_engine, pg_session)
        await _register_hook(pg_engine, pg_session, schema=SCHEMA_B)
        feature_store = PostgresFeatureStore(pg_engine, pg_session)
        await feature_store.insert_features(HOOK, str(srn_a), [{"score": 0.9, "label": "a"}])
        await feature_store.insert_features(HOOK, str(srn_b), [{"score": 0.2, "label": "b"}])
        return srn_a, srn_b

    async def test_stream_excludes_rows_of_other_schemas(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        srn_a, _ = await self._seed_shared_hook(pg_engine, pg_session)

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        rows = await _drain(rs, _feature_plan())

        assert [r["record_srn"] for r in rows] == [str(srn_a)]

    async def test_manifest_row_count_scoped_to_schema(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await self._seed_shared_hook(pg_engine, pg_session)

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        manifest = await rs.get_schema_manifest(SCHEMA)
        assert manifest is not None
        feature_res = next(t for t in manifest.table_resources if t.name == HOOK)
        assert feature_res.row_count == 1


@pytest.mark.asyncio
class TestFeatureCatalogAndManifest:
    async def test_manifest_includes_feature_resource(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        srn = await _publish(pg_engine, store, "rec1")
        await pg_session.commit()
        await _register_hook(pg_engine, pg_session)
        feature_store = PostgresFeatureStore(pg_engine, pg_session)
        await feature_store.insert_features(HOOK, str(srn), [{"score": 0.9, "label": "high"}])

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        manifest = await rs.get_schema_manifest(SCHEMA)
        assert manifest is not None
        feature_res = next(t for t in manifest.table_resources if t.name == HOOK)
        assert feature_res.kind == TK.FEATURE
        assert feature_res.row_count == 1
        col_names = [c.name for c in feature_res.columns]
        assert col_names[:3] == ["id", "record_srn", "created_at"]
        assert "score" in col_names and "label" in col_names
        assert feature_res.formats == ["", "csv", "csv.gz"]

    async def test_catalog_lists_feature_resource(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        await _register_hook(pg_engine, pg_session)

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        catalog = await rs.get_node_catalog()
        entry = next(e for e in catalog.schemas if e.id == "compound")
        names = {(t.name, t.kind) for t in entry.table_resources}
        assert ("records", TK.RECORDS) in names
        assert (HOOK, TK.FEATURE) in names
