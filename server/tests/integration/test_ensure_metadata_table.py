"""Integration tests for EnsureMetadataTable event handler."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.deposition.event.convention_registered import ConventionRegistered
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.metadata.handler.ensure_metadata_table import EnsureMetadataTable
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.schema import Schema
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.event import EventId
from osa.domain.shared.model.srn import ConventionSRN, SchemaId
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.metadata_table import METADATA_SCHEMA
from osa.infrastructure.persistence.repository.convention import PostgresConventionRepository
from osa.infrastructure.persistence.repository.schema import PostgresSemanticsSchemaRepository

SCHEMA_ID = "bio-sample"
SCHEMA_V1 = SchemaId.parse(f"{SCHEMA_ID}@1.0.0")
SCHEMA_V11 = SchemaId.parse(f"{SCHEMA_ID}@1.1.0")


def _fields_v1() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


def _fields_v11() -> list[FieldDefinition]:
    return _fields_v1() + [
        FieldDefinition(
            name="collection_site",
            type=FieldType.TEXT,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _seed_schema(
    session: AsyncSession, srn: SchemaId, fields: list[FieldDefinition], title: str = "bio_sample"
) -> None:
    repo = PostgresSemanticsSchemaRepository(session)
    await repo.save(Schema(id=srn, title=title, fields=fields, created_at=datetime.now(UTC)))


async def _seed_convention(session: AsyncSession, srn: ConventionSRN, schema_id: SchemaId) -> None:
    repo = PostgresConventionRepository(session)
    await repo.save(
        Convention(
            srn=srn,
            title="bio_sample_v1",
            description=None,
            schema_id=schema_id,
            file_requirements=FileRequirements(accepted_types=[], max_count=0, max_file_size=0),
            hooks=[],
            created_at=datetime.now(UTC),
        )
    )


def _event(
    convention_srn: ConventionSRN,
    schema_id: SchemaId,
    schema_fields: list[FieldDefinition],
) -> ConventionRegistered:
    return ConventionRegistered(
        id=EventId(uuid4()),
        convention_srn=convention_srn,
        schema_id=schema_id,
        schema_fields=schema_fields,
        hooks=[],
    )


async def _make_handler(pg_engine: AsyncEngine, pg_session: AsyncSession) -> EnsureMetadataTable:
    store = PostgresMetadataStore(pg_engine, pg_session)
    service = MetadataService(metadata_store=store)
    return EnsureMetadataTable(
        metadata_service=service,
        convention_repo=PostgresConventionRepository(pg_session),
    )


async def _catalog_row_count(engine: AsyncEngine) -> int:
    async with engine.begin() as conn:
        return int((await conn.execute(text("SELECT COUNT(*) FROM metadata_tables"))).scalar() or 0)


async def _table_columns(engine: AsyncEngine, pg_table: str) -> set[str]:
    async with engine.begin() as conn:
        result = await conn.execute(
            text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = :s AND table_name = :t"
            ),
            {"s": METADATA_SCHEMA, "t": pg_table},
        )
        return {row[0] for row in result.fetchall()}


@pytest.mark.asyncio
class TestEnsureMetadataTable:
    async def test_first_event_creates_table_and_catalog_row(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        conv_srn = ConventionSRN.parse("urn:osa:localhost:conv:conv-c1@1.0.0")
        await _seed_schema(pg_session, SCHEMA_V1, _fields_v1())
        await _seed_convention(pg_session, conv_srn, SCHEMA_V1)
        await pg_session.commit()

        handler = await _make_handler(pg_engine, pg_session)
        await handler.handle(_event(conv_srn, SCHEMA_V1, _fields_v1()))
        await pg_session.commit()

        assert await _catalog_row_count(pg_engine) == 1
        cols = await _table_columns(pg_engine, "bio_sample_v1")
        assert "species" in cols

    async def test_second_event_same_schema_is_noop(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        conv_a = ConventionSRN.parse("urn:osa:localhost:conv:conv-a1@1.0.0")
        conv_b = ConventionSRN.parse("urn:osa:localhost:conv:conv-b1@1.0.0")
        await _seed_schema(pg_session, SCHEMA_V1, _fields_v1())
        await _seed_convention(pg_session, conv_a, SCHEMA_V1)
        await _seed_convention(pg_session, conv_b, SCHEMA_V1)
        await pg_session.commit()

        handler = await _make_handler(pg_engine, pg_session)
        await handler.handle(_event(conv_a, SCHEMA_V1, _fields_v1()))
        await handler.handle(_event(conv_b, SCHEMA_V1, _fields_v1()))
        await pg_session.commit()

        # Still one catalog row, one table.
        assert await _catalog_row_count(pg_engine) == 1

    async def test_additive_bump_alters_table(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        conv_a = ConventionSRN.parse("urn:osa:localhost:conv:conv-a1@1.0.0")
        conv_b = ConventionSRN.parse("urn:osa:localhost:conv:conv-b1@1.0.0")
        await _seed_schema(pg_session, SCHEMA_V1, _fields_v1())
        await _seed_schema(pg_session, SCHEMA_V11, _fields_v11())
        await _seed_convention(pg_session, conv_a, SCHEMA_V1)
        await _seed_convention(pg_session, conv_b, SCHEMA_V11)
        await pg_session.commit()

        handler = await _make_handler(pg_engine, pg_session)
        await handler.handle(_event(conv_a, SCHEMA_V1, _fields_v1()))
        cols_before = await _table_columns(pg_engine, "bio_sample_v1")
        assert "collection_site" not in cols_before

        await handler.handle(_event(conv_b, SCHEMA_V11, _fields_v11()))
        await pg_session.commit()

        cols_after = await _table_columns(pg_engine, "bio_sample_v1")
        assert "collection_site" in cols_after
