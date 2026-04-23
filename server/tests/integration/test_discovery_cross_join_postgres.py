"""Integration tests for cross-domain JOINs between records ⋈ features in discovery."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.discovery.model.refs import FeatureFieldRef, MetadataFieldRef
from osa.domain.discovery.model.value import And, FilterOperator, Not, Predicate, SortOrder
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.persistence.adapter.discovery import PostgresDiscoveryReadStore
from osa.infrastructure.persistence.feature_store import PostgresFeatureStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore

from tests.integration.conftest import seed_record

SCHEMA_V1 = SchemaId.parse("bio-sample@1.0.0")
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
    await mstore.ensure_table(SCHEMA_V1, _metadata_fields())

    fstore = PostgresFeatureStore(pg_engine, pg_session)
    await fstore.create_table("cell_classifier", _feature_columns())

    repo = PostgresSemanticsSchemaRepository(pg_session)
    await repo.save(
        Schema(
            id=SCHEMA_V1,
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
        await seed_record(
            pg_engine,
            srn=str(srn),
            schema_id=SCHEMA_V1.id.root,
            schema_version=SCHEMA_V1.version.root,
        )
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
            schema_id=SCHEMA_V1,
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
                schema_id=SCHEMA_V1,
                convention_srn=None,
                text_fields=[],
                q=None,
                sort="published_at",
                order=SortOrder.DESC,
                cursor=None,
                limit=10,
                field_types=FIELD_TYPES,
            )


@pytest.fixture
async def seeded_with_missing_feature_row(pg_engine: AsyncEngine, pg_session: AsyncSession):
    """Seed a record with a metadata row but NO feature row, so the outer
    join produces NULL feature columns for that record."""
    from datetime import UTC, datetime

    from osa.domain.semantics.model.schema import Schema
    from osa.infrastructure.persistence.repository.schema import (
        PostgresSemanticsSchemaRepository,
    )

    mstore = PostgresMetadataStore(pg_engine, pg_session)
    await mstore.ensure_table(SCHEMA_V1, _metadata_fields())

    fstore = PostgresFeatureStore(pg_engine, pg_session)
    await fstore.create_table("cell_classifier", _feature_columns())

    repo = PostgresSemanticsSchemaRepository(pg_session)
    await repo.save(
        Schema(
            id=SCHEMA_V1,
            title="bio_sample",
            fields=_metadata_fields(),
            created_at=datetime.now(UTC),
        )
    )

    # rec-has-feature: has a feature row with confidence 0.95.
    # rec-no-feature: no feature row at all (outer join will produce NULLs).
    for rid, sp in [("rec-has-feature", "Homo sapiens"), ("rec-no-feature", "Mus musculus")]:
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
        await seed_record(
            pg_engine,
            srn=str(srn),
            schema_id=SCHEMA_V1.id.root,
            schema_version=SCHEMA_V1.version.root,
        )
        await mstore.insert(SCHEMA_V1, srn, {"species": sp})

    has_feature_srn = "urn:osa:localhost:rec:rec-has-feature@1"
    await fstore.insert_features("cell_classifier", has_feature_srn, [{"confidence": 0.95}])

    await pg_session.commit()


@pytest.mark.asyncio
class TestOuterJoinNullHandling:
    """Records without a feature row must not be silently dropped from NEQ/NOT
    predicates on feature columns — the outer join produces NULL, and naive
    SQL three-valued logic would exclude them."""

    async def test_neq_on_feature_column_includes_missing_rows(
        self,
        pg_engine: AsyncEngine,
        pg_session: AsyncSession,
        seeded_with_missing_feature_row,
    ):
        read_store = PostgresDiscoveryReadStore(pg_session)
        tree = Predicate(
            field=FeatureFieldRef(hook="cell_classifier", column="confidence"),
            op=FilterOperator.NEQ,
            value=0.95,
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
            field_types=FIELD_TYPES,
        )
        srns = {str(r.srn) for r in results}
        # rec-no-feature has no feature row → confidence is NULL → "!= 0.95"
        # should include it. rec-has-feature has confidence 0.95 → excluded.
        assert srns == {"urn:osa:localhost:rec:rec-no-feature@1"}

    async def test_not_on_feature_column_includes_missing_rows(
        self,
        pg_engine: AsyncEngine,
        pg_session: AsyncSession,
        seeded_with_missing_feature_row,
    ):
        read_store = PostgresDiscoveryReadStore(pg_session)
        tree = Not(
            operand=Predicate(
                field=FeatureFieldRef(hook="cell_classifier", column="confidence"),
                op=FilterOperator.EQ,
                value=0.95,
            )
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
            field_types=FIELD_TYPES,
        )
        srns = {str(r.srn) for r in results}
        # Same invariant as NEQ: NOT(confidence = 0.95) must surface the
        # record with a missing feature row.
        assert srns == {"urn:osa:localhost:rec:rec-no-feature@1"}
