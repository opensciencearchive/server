"""Integration tests for cross-domain JOINs between records ⋈ features in discovery."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.discovery.model.refs import FeatureFieldRef, MetadataFieldRef
from osa.domain.discovery.model.value import And, FilterOperator, Predicate, SortOrder
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN
from osa.infrastructure.persistence.adapter.discovery import PostgresDiscoveryReadStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore

from tests.integration.conftest import seed_record

SCHEMA_V1 = SchemaSRN.parse("urn:osa:localhost:schema:bio-sample@1.0.0")
FIELD_TYPES = {"species": FieldType.TEXT}


def _metadata_fields() -> list[FieldDefinition]:
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
        ColumnDef(name="confidence", json_type="number", required=True),
    ]


@pytest.fixture
async def seeded_both(pg_engine: AsyncEngine, pg_session: AsyncSession):
    from datetime import UTC, datetime

    from osa.domain.semantics.model.schema import Schema
    from osa.infrastructure.persistence.repository.schema import (
        PostgresSemanticsSchemaRepository,
    )

    mstore = PostgresMetadataStore(pg_engine, pg_session)
    await mstore.ensure_table(SCHEMA_V1, "bio_sample", _metadata_fields())

    fstore = PostgresFeatureStore(pg_engine, pg_session)
    await fstore.create_table("cell_classifier", _feature_columns())

    repo = PostgresSemanticsSchemaRepository(pg_session)
    await repo.save(
        Schema(
            srn=SCHEMA_V1,
            title="bio_sample",
            fields=_metadata_fields(),
            created_at=datetime.now(UTC),
        )
    )

    # r1: Homo sapiens + confidence 0.95
    # r2: Homo sapiens + confidence 0.5
    # r3: Mus musculus + confidence 0.95
    for rid, sp, conf in [
        ("rec-r1", "Homo sapiens", 0.95),
        ("rec-r2", "Homo sapiens", 0.5),
        ("rec-r3", "Mus musculus", 0.95),
    ]:
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
        await seed_record(pg_engine, srn=str(srn), schema_srn=str(SCHEMA_V1))
        await mstore.insert(SCHEMA_V1, srn, {"species": sp})
        await fstore.insert_features("cell_classifier", str(srn), [{"confidence": conf}])

    await pg_session.commit()
    return mstore, fstore


@pytest.mark.asyncio
class TestCrossDomainJoin:
    async def test_joined_intersection(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, seeded_both
    ):
        read_store = PostgresDiscoveryReadStore(pg_session)
        tree = And(
            operands=[
                Predicate(
                    field=MetadataFieldRef(field="species"),
                    op=FilterOperator.EQ,
                    value="Homo sapiens",
                ),
                Predicate(
                    field=FeatureFieldRef(hook="cell_classifier", column="confidence"),
                    op=FilterOperator.GT,
                    value=0.9,
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
            field_types=FIELD_TYPES,
        )
        srns = {str(r.srn) for r in results}
        assert srns == {"urn:osa:localhost:rec:rec-r1@1"}

    async def test_unknown_hook_raises(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, seeded_both
    ):
        read_store = PostgresDiscoveryReadStore(pg_session)
        tree = Predicate(
            field=FeatureFieldRef(hook="does_not_exist", column="anything"),
            op=FilterOperator.EQ,
            value=1,
        )
        with pytest.raises(ValidationError, match="Unknown feature hook"):
            await read_store.search_records(
                filter_expr=tree,
                schema_srn=SCHEMA_V1,
                convention_srn=None,
                text_fields=[],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=10,
                field_types=FIELD_TYPES,
            )
