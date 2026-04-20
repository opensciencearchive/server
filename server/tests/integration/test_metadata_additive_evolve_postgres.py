"""Integration tests for additive schema evolution end-to-end."""

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.metadata_table import METADATA_SCHEMA

from tests.integration.conftest import seed_record

IDENTITY = "urn:osa:localhost:schema:bio-sample"
SCHEMA_V10 = SchemaSRN.parse(f"{IDENTITY}@1.0.0")
SCHEMA_V11 = SchemaSRN.parse(f"{IDENTITY}@1.1.0")


def _fields_v10() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _fields_v11() -> list[FieldDefinition]:
    return _fields_v10() + [
        FieldDefinition(
            name="collection_site",
            type=FieldType.TEXT,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


@pytest.mark.asyncio
class TestAdditiveEvolvePipeline:
    async def test_old_row_null_new_row_typed(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        service = MetadataService(metadata_store=PostgresMetadataStore(pg_engine, pg_session))

        # Register v1.0.0 and publish a record.
        await service.ensure_table(SCHEMA_V10, "bio_sample", _fields_v10())
        r_old = RecordSRN.parse("urn:osa:localhost:rec:old@1")
        await seed_record(pg_engine, srn=str(r_old), schema_srn=str(SCHEMA_V10))
        await service.insert(SCHEMA_V10, r_old, {"species": "Mus musculus"})
        await pg_session.commit()

        # Bump to v1.1.0 (additive) and publish another record carrying the new field.
        await service.ensure_table(SCHEMA_V11, "bio_sample", _fields_v11())
        r_new = RecordSRN.parse("urn:osa:localhost:rec:new@1")
        await seed_record(pg_engine, srn=str(r_new), schema_srn=str(SCHEMA_V11))
        await service.insert(
            SCHEMA_V11, r_new, {"species": "Homo sapiens", "collection_site": "Lab A"}
        )
        await pg_session.commit()

        # Old row: NULL in new column.
        async with pg_engine.begin() as conn:
            old_site = (
                await conn.execute(
                    text(
                        f'SELECT collection_site FROM "{METADATA_SCHEMA}"."bio_sample_v1" '
                        f"WHERE record_srn = :srn"
                    ),
                    {"srn": str(r_old)},
                )
            ).scalar()
            new_site = (
                await conn.execute(
                    text(
                        f'SELECT collection_site FROM "{METADATA_SCHEMA}"."bio_sample_v1" '
                        f"WHERE record_srn = :srn"
                    ),
                    {"srn": str(r_new)},
                )
            ).scalar()
        assert old_site is None
        assert new_site == "Lab A"

    async def test_catalog_lineage_has_both_srns(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        service = MetadataService(metadata_store=PostgresMetadataStore(pg_engine, pg_session))
        await service.ensure_table(SCHEMA_V10, "bio_sample", _fields_v10())
        await service.ensure_table(SCHEMA_V11, "bio_sample", _fields_v11())

        async with pg_engine.begin() as conn:
            versions = (
                await conn.execute(
                    text(
                        "SELECT schema_versions FROM metadata_tables "
                        "WHERE schema_identity = :id AND schema_major = 1"
                    ),
                    {"id": IDENTITY},
                )
            ).scalar()
        assert str(SCHEMA_V10) in versions
        assert str(SCHEMA_V11) in versions
