"""Integration tests for /discovery/records with typed-table AND filters."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.discovery.model.refs import MetadataFieldRef
from osa.domain.discovery.model.value import And, FilterOperator, Predicate, SortOrder
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN
from osa.infrastructure.persistence.adapter.discovery import (
    PostgresDiscoveryReadStore,
    PostgresFieldDefinitionReader,
)
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore

from tests.integration.conftest import seed_record

SCHEMA_V1 = SchemaSRN.parse("urn:osa:localhost:schema:bio-sample@1.0.0")


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
        FieldDefinition(
            name="method",
            type=FieldType.TEXT,
            required=False,
            cardinality=Cardinality.EXACTLY_ONE,
        ),
    ]


async def _seed_schema_row(session: AsyncSession) -> None:
    """Seed the `schemas` row so the discovery field reader can resolve types."""
    from datetime import UTC, datetime

    from osa.domain.semantics.model.schema import Schema
    from osa.infrastructure.persistence.repository.schema import (
        PostgresSemanticsSchemaRepository,
    )

    repo = PostgresSemanticsSchemaRepository(session)
    await repo.save(
        Schema(srn=SCHEMA_V1, title="bio_sample", fields=_fields(), created_at=datetime.now(UTC))
    )


async def _publish(
    engine: AsyncEngine,
    session: AsyncSession,
    store: PostgresMetadataStore,
    record_srn: RecordSRN,
    species: str,
    resolution: float,
    method: str,
) -> None:
    await seed_record(
        engine,
        srn=str(record_srn),
        schema_srn=str(SCHEMA_V1),
        metadata={"species": species, "resolution": resolution, "method": method},
    )
    await store.insert(
        SCHEMA_V1,
        record_srn,
        {"species": species, "resolution": resolution, "method": method},
    )


@pytest.mark.asyncio
class TestDiscoveryTypedAnd:
    async def test_and_filter_returns_matching_records(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields())
        await _seed_schema_row(pg_session)

        rows = [
            ("rec-r1", "Homo sapiens", 3.5, "cryo-EM"),
            ("rec-r2", "Homo sapiens", 1.8, "X-ray"),
            ("rec-r3", "Mus musculus", 3.0, "cryo-EM"),
        ]
        for rid, sp, res, meth in rows:
            await _publish(
                pg_engine,
                pg_session,
                store,
                RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1"),
                sp,
                res,
                meth,
            )
        await pg_session.commit()

        read_store = PostgresDiscoveryReadStore(pg_session)
        tree = And(
            operands=[
                Predicate(
                    field=MetadataFieldRef(field="species"),
                    op=FilterOperator.EQ,
                    value="Homo sapiens",
                ),
                Predicate(
                    field=MetadataFieldRef(field="resolution"),
                    op=FilterOperator.GTE,
                    value=2.0,
                ),
            ]
        )

        results = await read_store.search_records(
            filter_expr=tree,
            schema_srn=SCHEMA_V1,
            convention_srn=None,
            text_fields=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
            field_types={
                "species": FieldType.TEXT,
                "resolution": FieldType.NUMBER,
                "method": FieldType.TEXT,
            },
        )

        srns = {str(r.srn) for r in results}
        assert srns == {"urn:osa:localhost:rec:rec-r1@1"}

    async def test_scalar_op_succeeds_on_unindexed_column(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """FR-020: scalar ops must NOT be rejected for lack of index."""
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields())
        await _seed_schema_row(pg_session)

        await _publish(
            pg_engine,
            pg_session,
            store,
            RecordSRN.parse("urn:osa:localhost:rec:rec-ra@1"),
            "Homo sapiens",
            3.5,
            "cryo-EM",
        )
        await pg_session.commit()

        read_store = PostgresDiscoveryReadStore(pg_session)
        results = await read_store.search_records(
            filter_expr=Predicate(
                field=MetadataFieldRef(field="method"),
                op=FilterOperator.CONTAINS,
                value="cryo",
            ),
            schema_srn=SCHEMA_V1,
            convention_srn=None,
            text_fields=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
            field_types={
                "species": FieldType.TEXT,
                "resolution": FieldType.NUMBER,
                "method": FieldType.TEXT,
            },
        )
        assert len(results) == 1


@pytest.mark.asyncio
class TestUnscopedListing:
    """When no schema_srn is passed, discovery should still return canonical
    JSONB metadata — the typed table is an optimization, not the sole source."""

    async def test_unscoped_predicate_filter_hits_jsonb(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """Filtering by a metadata field without schema_srn must compile
        against the canonical JSONB column (the Pockets frontend pattern:
        fetch-by-pdb_id without knowing the schema SRN)."""
        from osa.domain.discovery.model.refs import MetadataFieldRef
        from osa.domain.discovery.model.value import FilterOperator, Predicate

        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields())
        await _seed_schema_row(pg_session)

        # Two records with distinct pdb-like ids in JSONB; typed table row
        # written for completeness but not read by this test.
        for srn_id, species in [("rec-9x1w", "Homo sapiens"), ("rec-8abc", "Mus musculus")]:
            await _publish(
                pg_engine,
                pg_session,
                store,
                RecordSRN.parse(f"urn:osa:localhost:rec:{srn_id}@1"),
                species,
                3.5,
                "cryo-EM",
            )
        await pg_session.commit()

        read_store = PostgresDiscoveryReadStore(pg_session)
        results = await read_store.search_records(
            filter_expr=Predicate(
                field=MetadataFieldRef(field="species"),
                op=FilterOperator.EQ,
                value="Homo sapiens",
            ),
            schema_srn=None,
            convention_srn=None,
            text_fields=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
            field_types={"species": FieldType.TEXT},
        )
        srns = {str(r.srn) for r in results}
        assert srns == {"urn:osa:localhost:rec:rec-9x1w@1"}

    async def test_unscoped_listing_returns_jsonb_metadata(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, "bio_sample", _fields())
        await _seed_schema_row(pg_session)

        await _publish(
            pg_engine,
            pg_session,
            store,
            RecordSRN.parse("urn:osa:localhost:rec:rec-unscoped@1"),
            "Homo sapiens",
            3.5,
            "cryo-EM",
        )
        await pg_session.commit()

        read_store = PostgresDiscoveryReadStore(pg_session)
        results = await read_store.search_records(
            filter_expr=None,
            schema_srn=None,  # deliberately unscoped — exercises the JSONB path
            convention_srn=None,
            text_fields=[],
            q=None,
            sort="published_at",
            order=SortOrder.DESC,
            cursor=None,
            limit=10,
            field_types={},
        )
        assert len(results) == 1
        assert results[0].metadata == {
            "species": "Homo sapiens",
            "resolution": 3.5,
            "method": "cryo-EM",
        }


@pytest.mark.asyncio
class TestFieldDefinitionReader:
    async def test_get_fields_for_schema(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        await _seed_schema_row(pg_session)
        await pg_session.commit()

        reader = PostgresFieldDefinitionReader(pg_session)
        fields = await reader.get_fields_for_schema(SCHEMA_V1)
        assert fields["species"] == FieldType.TEXT
        assert fields["resolution"] == FieldType.NUMBER
