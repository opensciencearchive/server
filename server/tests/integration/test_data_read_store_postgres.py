"""Integration tests for PostgresDataReadStore against a real Postgres.

Exercises the unified /data/ engine end-to-end: records streaming via the
server-side cursor, filtered streaming, single-record-by-id, node catalog, and
schema manifest. Mirrors the discovery integration tests' seeding pattern
(schema row + dynamic metadata table + seeded records).

Skips automatically unless OSA_DATABASE__URL points at PostgreSQL.
"""

from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.data.model.filter import FilterOperator, MetadataFieldRef, Predicate
from osa.domain.data.model.query_plan import (
    PaginationParams,
    QueryPlan,
    SortDirection,
    SortSpec,
    TableKind,
)
from osa.domain.data.model.query_plan import TableKind as TK
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.ids import RecordId
from osa.domain.shared.model.srn import Domain, RecordSRN, SchemaId
from osa.infrastructure.data.postgres_data_read_store import PostgresDataReadStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.schema import (
    PostgresSemanticsSchemaRepository,
)

from tests.integration.conftest import seed_record

SCHEMA = SchemaId.parse("compound@1.0.0")


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="mw",
            type=FieldType.NUMBER,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _setup_schema(engine: AsyncEngine, session: AsyncSession) -> PostgresMetadataStore:
    store = PostgresMetadataStore(engine, session)
    await store.ensure_table(SCHEMA, _fields())
    repo = PostgresSemanticsSchemaRepository(session)
    await repo.save(
        Schema(id=SCHEMA, title="compound", fields=_fields(), created_at=datetime.now(UTC))
    )
    return store


async def _publish(
    engine: AsyncEngine,
    store: PostgresMetadataStore,
    rid: str,
    species: str,
    mw: float,
    published_at: datetime,
) -> None:
    srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
    await seed_record(
        engine,
        srn=str(srn),
        schema_id=SCHEMA.id.root,
        schema_version=SCHEMA.version.root,
        metadata={"species": species, "mw": mw},
        published_at=published_at,
    )
    await store.insert(SCHEMA, srn, {"species": species, "mw": mw})


def _records_plan(filter_expr=None, limit=50, cursor=None) -> QueryPlan:
    return QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        filter=filter_expr,
        pagination=PaginationParams(limit=limit, cursor=cursor),
    )


async def _drain(store: PostgresDataReadStore, plan: QueryPlan) -> list[dict]:
    return [dict(row) async for row in store.stream_rows(plan)]


@pytest.mark.asyncio
class TestStreamRecords:
    async def test_streams_all_rows_flattened(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _publish(
            pg_engine, store, "rec1", "Homo sapiens", 1.5, datetime(2026, 1, 3, tzinfo=UTC)
        )
        await _publish(
            pg_engine, store, "rec2", "Mus musculus", 2.0, datetime(2026, 1, 1, tzinfo=UTC)
        )
        await pg_session.commit()

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        rows = await _drain(rs, _records_plan())

        assert len(rows) == 2
        # Default RECORDS sort is created_at desc → r1 first.
        first = rows[0]
        assert first["id"] == "rec1"
        assert first["srn"] == "urn:osa:localhost:rec:rec1@1"
        assert first["schema_id"] == "compound@1.0.0"
        assert first["version"] == 1
        assert first["species"] == "Homo sapiens"
        assert first["mw"] == 1.5
        assert "created_at" in first

    async def test_metadata_filter_narrows_results(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _publish(
            pg_engine, store, "rec1", "Homo sapiens", 1.5, datetime(2026, 1, 3, tzinfo=UTC)
        )
        await _publish(
            pg_engine, store, "rec2", "Mus musculus", 2.0, datetime(2026, 1, 1, tzinfo=UTC)
        )
        await pg_session.commit()

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        plan = _records_plan(
            filter_expr=Predicate(
                field=MetadataFieldRef(field="species"),
                op=FilterOperator.EQ,
                value="Homo sapiens",
            )
        )
        rows = await _drain(rs, plan)
        assert [r["id"] for r in rows] == ["rec1"]

    async def test_empty_result_streams_nothing(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        assert await _drain(rs, _records_plan()) == []


@pytest.mark.asyncio
class TestGetRecordById:
    async def test_resolves_by_bare_id_with_srn(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        await _publish(
            pg_engine, store, "abc", "Homo sapiens", 1.5, datetime(2026, 1, 1, tzinfo=UTC)
        )
        await pg_session.commit()

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        rec = await rs.get_record_by_id(RecordId("abc"), None)
        assert rec is not None
        assert str(rec.id) == "abc"
        assert str(rec.srn) == "urn:osa:localhost:rec:abc@1"
        assert rec.schema_id.render() == "compound@1.0.0"
        assert rec.version == 1
        assert rec.metadata["species"] == "Homo sapiens"

    async def test_unknown_id_returns_none(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        assert await rs.get_record_by_id(RecordId("missing"), None) is None


@pytest.mark.asyncio
class TestCatalogAndManifest:
    async def test_node_catalog_lists_schema(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        catalog = await rs.get_node_catalog()
        assert catalog.node_domain == "localhost"
        ids = {(e.id, e.version) for e in catalog.schemas}
        assert ("compound", "1.0.0") in ids

    async def test_manifest_shape(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = await _setup_schema(pg_engine, pg_session)
        await _publish(
            pg_engine, store, "rec1", "Homo sapiens", 1.5, datetime(2026, 1, 1, tzinfo=UTC)
        )
        await pg_session.commit()

        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        manifest = await rs.get_schema_manifest(SCHEMA)
        assert manifest is not None
        assert manifest.id == "compound"
        assert manifest.version == "1.0.0"
        assert {f.name for f in manifest.fields} == {"species", "mw"}
        records_res = next(t for t in manifest.table_resources if t.name == "records")
        assert records_res.kind == TK.RECORDS
        assert records_res.row_count == 1
        col_names = [c.name for c in records_res.columns]
        # implicit columns precede the declared fields
        assert col_names[:5] == ["id", "srn", "schema_id", "version", "created_at"]
        assert "species" in col_names and "mw" in col_names
        assert records_res.formats == ["", "csv", "csv.gz"]

    async def test_get_latest_schema_id(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _setup_schema(pg_engine, pg_session)
        await pg_session.commit()
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))
        resolved = await rs.get_latest_schema_id("compound")
        assert resolved is not None
        assert resolved.render() == "compound@1.0.0"
        assert await rs.get_latest_schema_id("nonexistent") is None


@pytest.mark.asyncio
class TestStreamPaginationOrder:
    async def test_cursor_advances_without_overlap(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = await _setup_schema(pg_engine, pg_session)
        for i in range(5):
            await _publish(
                pg_engine,
                store,
                f"rec{i}",
                "Homo sapiens",
                float(i),
                datetime(2026, 1, 1 + i, tzinfo=UTC),
            )
        await pg_session.commit()
        rs = PostgresDataReadStore(pg_session, Domain("localhost"))

        # Sort by created_at desc (default). Page 1: take 2, derive cursor from row 2.
        from osa.domain.data.model.query_plan import PaginationCursor, encode_cursor

        all_rows = await _drain(rs, _records_plan())
        assert [r["id"] for r in all_rows] == ["rec4", "rec3", "rec2", "rec1", "rec0"]

        # Cursor after the 2nd row (r3) → next page should start at r2.
        cursor = encode_cursor(all_rows[1]["created_at"], all_rows[1]["srn"])
        plan2 = QueryPlan(
            schema_id=SCHEMA,
            table_kind=TableKind.RECORDS,
            pagination=PaginationParams(limit=50, cursor=PaginationCursor(value=cursor)),
            sort=[SortSpec(column="created_at", direction=SortDirection.DESC)],
        )
        page2 = await _drain(rs, plan2)
        assert [r["id"] for r in page2] == ["rec2", "rec1", "rec0"]
