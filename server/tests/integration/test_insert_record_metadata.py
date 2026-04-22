"""Integration tests for InsertRecordMetadata event handler."""

from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.metadata.handler.insert_record_metadata import InsertRecordMetadata
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.event import EventId
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN, SchemaId
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.metadata_table import METADATA_SCHEMA

from tests.integration.conftest import seed_record

SCHEMA_V1 = SchemaId.parse("bio-sample@1.0.0")
CONV_SRN = ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _fields() -> list[FieldDefinition]:
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


def _event(record_srn: RecordSRN, metadata: dict) -> RecordPublished:
    return RecordPublished(
        id=EventId(uuid4()),
        record_srn=record_srn,
        source=DepositionSource(id="dep-1"),
        convention_srn=CONV_SRN,
        schema_id=SCHEMA_V1,
        metadata=metadata,
        expected_features=[],
    )


@pytest.mark.asyncio
class TestInsertRecordMetadata:
    async def test_insert_creates_typed_row(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, _fields())

        record_srn = RecordSRN.parse("urn:osa:localhost:rec:one@1")
        await seed_record(
            pg_engine,
            srn=str(record_srn),
            schema_id=SCHEMA_V1.id.root,
            schema_version=SCHEMA_V1.version.root,
        )

        handler = InsertRecordMetadata(metadata_service=MetadataService(metadata_store=store))
        await handler.handle(_event(record_srn, {"species": "Homo sapiens", "resolution": 3.5}))
        await pg_session.commit()

        async with pg_engine.begin() as conn:
            row = (
                await conn.execute(
                    text(
                        f"SELECT species, resolution "
                        f'FROM "{METADATA_SCHEMA}"."bio_sample_v1" '
                        f"WHERE record_srn = :srn"
                    ),
                    {"srn": str(record_srn)},
                )
            ).first()
        assert row is not None
        assert row[0] == "Homo sapiens"
        assert row[1] == 3.5

    async def test_duplicate_delivery_is_idempotent(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, _fields())

        record_srn = RecordSRN.parse("urn:osa:localhost:rec:dup@1")
        await seed_record(
            pg_engine,
            srn=str(record_srn),
            schema_id=SCHEMA_V1.id.root,
            schema_version=SCHEMA_V1.version.root,
        )

        handler = InsertRecordMetadata(metadata_service=MetadataService(metadata_store=store))
        event = _event(record_srn, {"species": "Mus musculus", "resolution": 1.0})

        await handler.handle(event)
        await handler.handle(event)
        await pg_session.commit()

        async with pg_engine.begin() as conn:
            count = (
                await conn.execute(
                    text(f'SELECT COUNT(*) FROM "{METADATA_SCHEMA}"."bio_sample_v1"')
                )
            ).scalar()
        assert count == 1
