"""Integration tests for dual-write of records + typed metadata.

``RecordService.bulk_publish`` and ``RecordService.publish_record`` now write
both the canonical ``records`` row and the typed ``metadata.<slug>_v<major>``
row atomically in one transaction. These tests verify:

- Both rows land on a successful publish.
- A malformed metadata value rolls back the whole transaction — no partial
  state where ``records`` has a row but the typed table doesn't.
- ``ConventionService.deploy`` creates the typed table inline
  (no event-handler race window).

Feature #145: conventions are slug-keyed and unversioned; ``create_convention``
was replaced by the bundled ``deploy`` (schema + hooks + convention in one
transaction). The convention is addressed by its bare ``ConventionSlug`` (the
``conventions.id`` column), no longer an opaque SRN.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from tests.factories import make_convention_docs
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.service.convention import ConventionService
from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.service import RecordService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.semantics.service.schema import SchemaService
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import ConventionSlug, Domain, SchemaIdentifier
from osa.domain.validation.service.hook_registry import HookRegistryService
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore
from osa.infrastructure.persistence.repository.convention import PostgresConventionRepository
from osa.infrastructure.persistence.repository.hook_registry import PostgresHookRegistry
from osa.infrastructure.persistence.repository.ontology import PostgresOntologyRepository
from osa.infrastructure.persistence.repository.record import PostgresRecordRepository
from osa.infrastructure.persistence.repository.schema import PostgresSemanticsSchemaRepository


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


async def _register_convention(
    pg_engine: AsyncEngine,
    pg_session: AsyncSession,
    slug: str = "dual-write-sample",
) -> ConventionService:
    metadata_store = PostgresMetadataStore(pg_engine, pg_session)
    metadata_service = MetadataService(metadata_store=metadata_store)
    schema_service = SchemaService(
        schema_repo=PostgresSemanticsSchemaRepository(pg_session),
        ontology_repo=PostgresOntologyRepository(pg_session),
        node_domain=Domain("localhost"),
    )
    convention_service = ConventionService(
        convention_repo=PostgresConventionRepository(pg_session),
        schema_service=schema_service,
        metadata_service=metadata_service,
        hook_registry=HookRegistryService(registry=PostgresHookRegistry(pg_session)),
        outbox=AsyncMock(),
        node_domain=Domain("localhost"),
    )
    # Bundled deploy: schema + typed metadata table + convention, one txn.
    # Use the slug for both the convention and its schema so the typed table is
    # named "<slug>_v1" (asserted below).
    await convention_service.deploy(
        slug=ConventionSlug.parse(slug),
        title="Dual Write Sample",
        description="Dual write sample convention",
        file_requirements=FileRequirements(accepted_types=[], max_count=0, max_file_size=0),
        schema_slug=SchemaIdentifier(slug),
        schema_version="1.0.0",
        schema_fields=_fields(),
        docs=make_convention_docs(),
    )
    await pg_session.commit()
    return convention_service


@pytest.mark.asyncio
class TestConventionCreatesTypedTableInline:
    async def test_typed_table_exists_immediately_after_deploy(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """No event-handler race window — the table exists in the same txn."""
        await _register_convention(pg_engine, pg_session, slug="inline-create")

        async with pg_engine.begin() as conn:
            exists = (
                await conn.execute(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
                        "WHERE table_schema = 'metadata' AND table_name = 'inline_create_v1')"
                    )
                )
            ).scalar()
        assert exists is True


@pytest.mark.asyncio
class TestBulkPublishDualWrite:
    async def test_bulk_publish_writes_both_tables(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        await _register_convention(pg_engine, pg_session, slug="bulk-dual")

        record_service = RecordService(
            record_repo=PostgresRecordRepository(pg_session),
            convention_repo=PostgresConventionRepository(pg_session),
            metadata_service=MetadataService(
                metadata_store=PostgresMetadataStore(pg_engine, pg_session),
            ),
            outbox=AsyncMock(),
            node_domain=Domain("localhost"),
            feature_reader=AsyncMock(),
        )

        # Drafts reference the convention by its bare slug (#145).
        drafts = [
            RecordDraft(
                source=DepositionSource(id=f"dep-{uuid4()}"),
                metadata={"species": "Homo sapiens", "resolution": 2.0 + i * 0.1},
                convention_id=ConventionSlug.parse("bulk-dual"),
            )
            for i in range(3)
        ]

        published = await record_service.bulk_publish(drafts)
        await pg_session.commit()

        assert len(published) == 3

        async with pg_engine.begin() as conn:
            records_count = (
                await conn.execute(
                    text("SELECT COUNT(*) FROM records WHERE schema_id = 'bulk-dual'")
                )
            ).scalar()
            typed_count = (
                await conn.execute(text('SELECT COUNT(*) FROM "metadata"."bulk_dual_v1"'))
            ).scalar()
        assert records_count == 3
        assert typed_count == 3

    async def test_malformed_metadata_rolls_back_everything(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """A type error in the typed write must fail the whole transaction —
        no orphan row left in ``records``."""
        await _register_convention(pg_engine, pg_session, slug="rollback-sample")

        record_service = RecordService(
            record_repo=PostgresRecordRepository(pg_session),
            convention_repo=PostgresConventionRepository(pg_session),
            metadata_service=MetadataService(
                metadata_store=PostgresMetadataStore(pg_engine, pg_session),
            ),
            outbox=AsyncMock(),
            node_domain=Domain("localhost"),
            feature_reader=AsyncMock(),
        )

        # 'resolution' expects a NUMBER; pass a non-coercible string.
        drafts = [
            RecordDraft(
                source=DepositionSource(id=f"dep-{uuid4()}"),
                metadata={"species": "A", "resolution": "not-a-number"},
                convention_id=ConventionSlug.parse("rollback-sample"),
            )
        ]

        with pytest.raises(Exception):  # noqa: BLE001 — asyncpg DataError or similar
            await record_service.bulk_publish(drafts)
            await pg_session.commit()
        await pg_session.rollback()

        async with pg_engine.begin() as conn:
            records_count = (
                await conn.execute(
                    text("SELECT COUNT(*) FROM records WHERE schema_id = 'rollback-sample'")
                )
            ).scalar()
            typed_count = (
                await conn.execute(text('SELECT COUNT(*) FROM "metadata"."rollback_sample_v1"'))
            ).scalar()
        assert records_count == 0
        assert typed_count == 0
