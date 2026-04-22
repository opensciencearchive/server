"""Integration tests for /discovery/records with typed-table AND filters."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.discovery.model.refs import MetadataFieldRef
from osa.domain.discovery.model.value import And, FilterOperator, Predicate, SortOrder
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.persistence.adapter.discovery import (
    PostgresDiscoveryReadStore,
    PostgresFieldDefinitionReader,
)
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore

from tests.integration.conftest import seed_record

SCHEMA_V1 = SchemaId.parse("bio-sample@1.0.0")


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
        Schema(id=SCHEMA_V1, title="bio_sample", fields=_fields(), created_at=datetime.now(UTC))
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
        schema_id=SCHEMA_V1.id.root,
        schema_version=SCHEMA_V1.version.root,
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
        await store.ensure_table(SCHEMA_V1, _fields())
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
            schema_id=SCHEMA_V1,
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
        await store.ensure_table(SCHEMA_V1, _fields())
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
            schema_id=SCHEMA_V1,
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
    """Plain listings without a filter return canonical JSONB metadata.
    Metadata-filtered queries require a pinned schema — the typed table is
    the only filter path."""

    async def test_unscoped_predicate_filter_raises_without_schema(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        """Filtering by a metadata field without schema_id must raise —
        the JSONB fallback compile path was removed."""
        from osa.domain.discovery.model.refs import MetadataFieldRef
        from osa.domain.discovery.model.value import FilterOperator, Predicate
        from osa.domain.shared.error import ValidationError

        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, _fields())
        await _seed_schema_row(pg_session)
        await _publish(
            pg_engine,
            pg_session,
            store,
            RecordSRN.parse("urn:osa:localhost:rec:rec-9x1w@1"),
            "Homo sapiens",
            3.5,
            "cryo-EM",
        )
        await pg_session.commit()

        read_store = PostgresDiscoveryReadStore(pg_session)
        with pytest.raises(ValidationError) as exc:
            await read_store.search_records(
                filter_expr=Predicate(
                    field=MetadataFieldRef(field="species"),
                    op=FilterOperator.EQ,
                    value="Homo sapiens",
                ),
                schema_id=None,
                convention_srn=None,
                text_fields=[],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=10,
                field_types={"species": FieldType.TEXT},
            )
        assert exc.value.code == "schema_required_for_metadata_query"

    async def test_unscoped_listing_returns_jsonb_metadata(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        store = PostgresMetadataStore(pg_engine, pg_session)
        await store.ensure_table(SCHEMA_V1, _fields())
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
            schema_id=None,  # deliberately unscoped — exercises the JSONB path
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
