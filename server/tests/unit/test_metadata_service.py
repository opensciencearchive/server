"""MetadataService unit tests — thin delegator over MetadataStore."""

from unittest.mock import AsyncMock

from osa.domain.metadata.service.metadata import MetadataService
from osa.domain.semantics.model.value import Cardinality, FieldDefinition, FieldType
from osa.domain.shared.model.srn import RecordSRN, SchemaId

SCHEMA = SchemaId.parse("bio-sample@1.0.0")
RECORD = RecordSRN.parse("urn:osa:localhost:rec:abc@1")


def _fields() -> list[FieldDefinition]:
    return [
        FieldDefinition(
            name="species",
            type=FieldType.TEXT,
            required=True,
            cardinality=Cardinality.EXACTLY_ONE,
        )
    ]


class TestMetadataService:
    async def test_ensure_table_delegates(self):
        store = AsyncMock()
        svc = MetadataService(metadata_store=store)
        await svc.ensure_table(schema_id=SCHEMA, fields=_fields())
        store.ensure_table.assert_called_once()

    async def test_insert_delegates(self):
        store = AsyncMock()
        svc = MetadataService(metadata_store=store)
        await svc.insert(
            schema_id=SCHEMA,
            record_srn=RECORD,
            values={"species": "Homo sapiens"},
        )
        store.insert.assert_called_once()
