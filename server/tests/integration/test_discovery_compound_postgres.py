"""Integration tests for compound OR/NOT discovery filters against real PG."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.discovery.model.refs import MetadataFieldRef
from osa.domain.discovery.model.value import (
    And,
    FilterOperator,
    Not,
    Or,
    Predicate,
    SortOrder,
)
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.persistence.adapter.discovery import PostgresDiscoveryReadStore
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore

from tests.integration.conftest import seed_record

SCHEMA_V1 = SchemaId.parse("bio-sample@1.0.0")
FIELD_TYPES = {"species": FieldType.TEXT, "resolution": FieldType.NUMBER}


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


@pytest.fixture
async def seeded_store(pg_engine: AsyncEngine, pg_session: AsyncSession) -> PostgresMetadataStore:
    from datetime import UTC, datetime

    from osa.domain.semantics.model.schema import Schema
    from osa.infrastructure.persistence.repository.schema import (
        PostgresSemanticsSchemaRepository,
    )

    store = PostgresMetadataStore(pg_engine, pg_session)
    await store.ensure_table(SCHEMA_V1, _fields())

    repo = PostgresSemanticsSchemaRepository(pg_session)
    await repo.save(
        Schema(id=SCHEMA_V1, title="bio_sample", fields=_fields(), created_at=datetime.now(UTC))
    )

    rows = [
        ("rec-a1", "Homo sapiens", 3.5),
        ("rec-b1", "Homo sapiens", 1.0),
        ("rec-c1", "Mus musculus", 3.5),
        ("rec-d1", "Drosophila", 0.5),
    ]
    for rid, sp, res in rows:
        srn = RecordSRN.parse(f"urn:osa:localhost:rec:{rid}@1")
        await seed_record(
            pg_engine,
            srn=str(srn),
            schema_id=SCHEMA_V1.id.root,
            schema_version=SCHEMA_V1.version.root,
        )
        await store.insert(SCHEMA_V1, srn, {"species": sp, "resolution": res})

    await pg_session.commit()
    return store


def _pred(field: str, op: FilterOperator, value: object) -> Predicate:
    return Predicate(field=MetadataFieldRef(field=field), op=op, value=value)


@pytest.mark.asyncio
class TestCompound:
    async def test_or_tree(self, pg_engine: AsyncEngine, pg_session: AsyncSession, seeded_store):
        read_store = PostgresDiscoveryReadStore(pg_session)
        # species = Homo sapiens OR resolution < 1.0
        tree = Or(
            operands=[
                _pred("species", FilterOperator.EQ, "Homo sapiens"),
                _pred("resolution", FilterOperator.LT, 1.0),
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
        # a, b (species match) + d (resolution 0.5) — not c
        assert srns == {
            "urn:osa:localhost:rec:rec-a1@1",
            "urn:osa:localhost:rec:rec-b1@1",
            "urn:osa:localhost:rec:rec-d1@1",
        }

    async def test_not_tree(self, pg_engine: AsyncEngine, pg_session: AsyncSession, seeded_store):
        read_store = PostgresDiscoveryReadStore(pg_session)
        tree = Not(operand=_pred("species", FilterOperator.EQ, "Homo sapiens"))
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
        assert srns == {"urn:osa:localhost:rec:rec-c1@1", "urn:osa:localhost:rec:rec-d1@1"}

    async def test_nested_and_or(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession, seeded_store
    ):
        read_store = PostgresDiscoveryReadStore(pg_session)
        # resolution >= 3.0 AND (species = Homo sapiens OR species = Mus musculus)
        tree = And(
            operands=[
                _pred("resolution", FilterOperator.GTE, 3.0),
                Or(
                    operands=[
                        _pred("species", FilterOperator.EQ, "Homo sapiens"),
                        _pred("species", FilterOperator.EQ, "Mus musculus"),
                    ]
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
        assert srns == {"urn:osa:localhost:rec:rec-a1@1", "urn:osa:localhost:rec:rec-c1@1"}
