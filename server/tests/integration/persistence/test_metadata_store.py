"""Integration tests for PostgresMetadataStore — DDL, UPSERT, FK cascade, additive evolution."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.metadata_table import METADATA_SCHEMA

from tests.integration.conftest import seed_record

SCHEMA_IDENTITY = "urn:osa:localhost:schema:bio-sample"
SCHEMA_V1 = SchemaSRN.parse(f"{SCHEMA_IDENTITY}@1.0.0")
SCHEMA_V11 = SchemaSRN.parse(f"{SCHEMA_IDENTITY}@1.1.0")
SCHEMA_V2 = SchemaSRN.parse(f"{SCHEMA_IDENTITY}@2.0.0")


def _fields_v1() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="resolution",
            type=FieldType.NUMBER,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _fields_v11_additive() -> list[FieldDefinition]:
    return _fields_v1() + [
        FieldDefinition(
            name="collection_site",
            type=FieldType.TEXT,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _fields_rename() -> list[FieldDefinition]:
    # 'species' renamed to 'organism' — not additive.
    return [
        FieldDefinition(
            name="organism",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
        FieldDefinition(
            name="resolution",
            type=FieldType.NUMBER,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _table_exists(engine: AsyncEngine, pg_table: str) -> bool:
    async with engine.begin() as conn:
        result = await conn.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :s AND table_name = :t)"
            ),
            {"s": METADATA_SCHEMA, "t": pg_table},
        )
        return bool(result.scalar())


async def _column_names(engine: AsyncEngine, pg_table: str) -> list[str]:
    async with engine.begin() as conn:
        result = await conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t "
                "ORDER BY ordinal_position"
            ),
            {"s": METADATA_SCHEMA, "t": pg_table},
        )
        return [row[0] for row in result.fetchall()]


@pytest.mark.asyncio
class TestEnsureTable:
    async def test_creates_table_and_catalog_row(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        assert await _table_exists(pg_engine, "bio_sample_v1")
        cols = await _column_names(pg_engine, "bio_sample_v1")
        for expected in ("id", "record_srn", "created_at", "species", "resolution"):
            assert expected in cols

        async with pg_engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT schema_identity, schema_major, pg_table, schema_versions "
                        "FROM metadata_tables WHERE schema_identity = :id"
                    ),
                    {"id": SCHEMA_IDENTITY},
                )
            ).first()
        assert row is not None
        assert row[0] == SCHEMA_IDENTITY
        assert row[1] == 1
        assert row[2] == "bio_sample_v1"
        assert str(SCHEMA_V1) in row[3]

    async def test_idempotent_on_same_version(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())
        # Second call with same SRN should not raise and should not duplicate catalog rows.
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        async with pg_engine.begin() as conn:
            count = (
                await conn.execute(
                    text("SELECT COUNT(*) FROM metadata_tables WHERE schema_identity = :id"),
                    {"id": SCHEMA_IDENTITY},
                )
            ).scalar()
        assert count == 1

    async def test_foreign_key_cascade_on_record_srn(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        async with pg_engine.begin() as conn:
            constraint = (
                await conn.execute(
                    text(
                        "SELECT confdeltype FROM pg_constraint "
                        "WHERE conrelid = 'metadata.bio_sample_v1'::regclass "
                        "AND contype = 'f'"
                    )
                )
            ).scalar()
        # 'c' = CASCADE in pg_constraint.confdeltype. asyncpg returns the
        # Postgres "char" type as bytes; normalize for comparison.
        if isinstance(constraint, bytes):
            constraint = constraint.decode()
        assert constraint == "c"


@pytest.mark.asyncio
class TestInsert:
    async def test_insert_typed_row(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        record_srn = RecordSRN.parse("urn:osa:localhost:rec:abc@1")
        await seed_record(pg_engine, srn=str(record_srn), schema_srn=str(SCHEMA_V1))

        await store.insert(
            SCHEMA_V1,
            record_srn,
            {"species": "Homo sapiens", "resolution": 3.5},
        )
        await pg_session.commit()

        async with pg_engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        f"SELECT record_srn, species, resolution "
                        f'FROM "{METADATA_SCHEMA}"."bio_sample_v1"'
                    )
                )
            ).first()
        assert row is not None
        assert row[0] == str(record_srn)
        assert row[1] == "Homo sapiens"
        assert row[2] == 3.5

    async def test_insert_is_idempotent_on_duplicate_delivery(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        record_srn = RecordSRN.parse("urn:osa:localhost:rec:dup@1")
        await seed_record(pg_engine, srn=str(record_srn), schema_srn=str(SCHEMA_V1))

        await store.insert(SCHEMA_V1, record_srn, {"species": "Mus musculus", "resolution": 1.0})
        await store.insert(SCHEMA_V1, record_srn, {"species": "Mus musculus", "resolution": 1.0})
        await pg_session.commit()

        async with pg_engine.begin() as conn:
            count = (
                await conn.execute(
                    text(f'SELECT COUNT(*) FROM "{METADATA_SCHEMA}"."bio_sample_v1"')
                )
            ).scalar()
        assert count == 1

    async def test_cascade_delete_removes_metadata_row(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        record_srn = RecordSRN.parse("urn:osa:localhost:rec:cascade@1")
        await seed_record(pg_engine, srn=str(record_srn), schema_srn=str(SCHEMA_V1))
        await store.insert(SCHEMA_V1, record_srn, {"species": "Cascade", "resolution": 0.1})
        await pg_session.commit()

        async with pg_engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM records WHERE srn = :srn"), {"srn": str(record_srn)}
            )

        async with pg_engine.begin() as conn:
            count = (
                await conn.execute(
                    text(f'SELECT COUNT(*) FROM "{METADATA_SCHEMA}"."bio_sample_v1"')
                )
            ).scalar()
        assert count == 0


@pytest.mark.asyncio
class TestAdditiveEvolution:
    async def test_add_column_on_minor_bump(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())
        cols_before = await _column_names(pg_engine, "bio_sample_v1")
        assert "collection_site" not in cols_before

        await store.ensure_table(SCHEMA_V11, "bio_sample", _fields_v11_additive())
        cols_after = await _column_names(pg_engine, "bio_sample_v1")
        assert "collection_site" in cols_after

    async def test_catalog_lineage_appended(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())
        await store.ensure_table(SCHEMA_V11, "bio_sample", _fields_v11_additive())

        async with pg_engine.begin() as conn:
            versions = (
                await conn.execute(
                    text(
                        "SELECT schema_versions FROM metadata_tables "
                        "WHERE schema_identity = :id AND schema_major = 1"
                    ),
                    {"id": SCHEMA_IDENTITY},
                )
            ).scalar()
        assert str(SCHEMA_V1) in versions
        assert str(SCHEMA_V11) in versions


@pytest.mark.asyncio
class TestNonAdditiveRejection:
    async def test_rename_raises(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        with pytest.raises(ValidationError, match="Non-additive"):
            await store.ensure_table(SCHEMA_V11, "bio_sample", _fields_rename())

    async def test_required_new_field_raises(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields_v1())

        bad = _fields_v1() + [
            FieldDefinition(
                name="must_have",
                type=FieldType.TEXT,
                required=True,
                cardinality=Cardinality.EXACTLY_ONE,
            )
        ]
        with pytest.raises(ValidationError, match="required"):
            await store.ensure_table(SCHEMA_V11, "bio_sample", bad)
