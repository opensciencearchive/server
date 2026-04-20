"""Integration tests for non-additive schema evolution rejection (FR-023)."""

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import SchemaSRN
from osa.infrastructure.persistence.metadata_store import PostgresMetadataStore

IDENTITY = "urn:osa:localhost:schema:bio-sample"
V1 = SchemaSRN.parse(f"{IDENTITY}@1.0.0")
V11 = SchemaSRN.parse(f"{IDENTITY}@1.1.0")


def _orig() -> list[FieldDefinition]:
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


@pytest.mark.asyncio
class TestNonAdditiveRejected:
    async def test_rename_field_rejected(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        svc = MetadataService(metadata_store=PostgresMetadataStore(pg_engine, pg_session))
        await svc.ensure_table(V1, "bio_sample", _orig())

        # New field "organism" is optional so the validator reaches the removal
        # check and reports the dropped "species" field specifically.
        renamed = [
            FieldDefinition(
                name="organism",
                type=FieldType.TEXT,
                required=False,
                cardinality=Cardinality.EXACTLY_ONE,
            ),
            FieldDefinition(
                name="resolution",
                type=FieldType.NUMBER,
                required=False,
                cardinality=Cardinality.EXACTLY_ONE,
            ),
        ]
        with pytest.raises(ValidationError) as exc:
            await svc.ensure_table(V11, "bio_sample", renamed)
        message = str(exc.value)
        assert "species" in message and "removed" in message

    async def test_type_change_rejected(self, pg_engine: AsyncEngine, pg_session: AsyncSession):
        svc = MetadataService(metadata_store=PostgresMetadataStore(pg_engine, pg_session))
        await svc.ensure_table(V1, "bio_sample", _orig())

        retyped = [
            FieldDefinition(
                name="species",
                type=FieldType.TEXT,
                required=True,
                cardinality=Cardinality.EXACTLY_ONE,
            ),
            FieldDefinition(
                name="resolution",
                # Previously NUMBER, now TEXT — retype is non-additive.
                type=FieldType.TEXT,
                required=False,
                cardinality=Cardinality.EXACTLY_ONE,
            ),
        ]
        with pytest.raises(ValidationError, match="resolution"):
            await svc.ensure_table(V11, "bio_sample", retyped)

    async def test_tightening_required_rejected(
        self, pg_engine: AsyncEngine, pg_session: AsyncSession
    ):
        svc = MetadataService(metadata_store=PostgresMetadataStore(pg_engine, pg_session))
        await svc.ensure_table(V1, "bio_sample", _orig())

        tightened = [
            FieldDefinition(
                name="species",
                type=FieldType.TEXT,
                required=True,
                cardinality=Cardinality.EXACTLY_ONE,
            ),
            FieldDefinition(
                name="resolution",
                type=FieldType.NUMBER,
                required=True,  # was False
                cardinality=Cardinality.EXACTLY_ONE,
            ),
        ]
        with pytest.raises(ValidationError, match="resolution"):
            await svc.ensure_table(V11, "bio_sample", tightened)
