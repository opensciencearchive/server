"""DataCatalogService.resolve_table — one owner for "which columns does this table have".

The records/feature route files previously each held a private helper that
re-derived manifest structure (records resource is named "records"; a feature
resource has kind FEATURE). That knowledge belongs to the catalog service.
"""

from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest

from osa.domain.data.model.catalog import NodeCatalog
from osa.domain.data.model.manifest import (
    ColumnSpec,
    ResolvedTable,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import QueryPlan, TableKind
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.data.service.data_catalog import DataCatalogService
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.ids import HookName, RecordId
from osa.domain.shared.model.srn import SchemaId

RECORDS_COLUMNS = [
    ColumnSpec(name="id", type=FieldType.TEXT),
    ColumnSpec(name="srn", type=FieldType.TEXT),
]
FEATURE_COLUMNS = [
    ColumnSpec(name="id", type=FieldType.NUMBER),
    ColumnSpec(name="mw", type=FieldType.NUMBER),
]

MANIFEST = SchemaManifest(
    id="compound",
    version="1.0.0",
    srn="urn:osa:localhost:schema:compound@1.0.0",
    fields=[],
    table_resources=[
        TableResource(
            name="records",
            kind=TableKind.RECORDS,
            columns=RECORDS_COLUMNS,
            row_count=2,
            formats=["", "csv", "csv.gz"],
        ),
        TableResource(
            name="chem_features",
            kind=TableKind.FEATURE,
            columns=FEATURE_COLUMNS,
            row_count=2,
            formats=["", "csv", "csv.gz"],
        ),
    ],
)


class FakeReadStore:
    """Minimal DataReadStore fake for catalog reads."""

    def __init__(self, manifest: SchemaManifest | None = MANIFEST) -> None:
        self.manifest = manifest

    def stream_rows(self, plan: QueryPlan) -> AsyncIterator[Mapping[str, Any]]:
        raise NotImplementedError

    async def get_record_by_id(self, id: RecordId, version: int | None) -> RecordSummary | None:
        return None

    async def get_node_catalog(self) -> NodeCatalog:
        raise NotImplementedError

    async def get_schema_manifest(self, schema_id: SchemaId) -> SchemaManifest | None:
        return self.manifest

    async def get_latest_schema_id(self, schema_short_id: str) -> SchemaId | None:
        return SchemaId.parse(f"{schema_short_id}@1.0.0")


def _service(store: FakeReadStore | None = None) -> DataCatalogService:
    return DataCatalogService(read_store=store or FakeReadStore())


@pytest.mark.asyncio
async def test_resolve_table_records() -> None:
    resolved = await _service().resolve_table("compound@1.0.0", TableKind.RECORDS)
    assert isinstance(resolved, ResolvedTable)
    assert resolved.schema_id == SchemaId.parse("compound@1.0.0")
    assert resolved.columns == RECORDS_COLUMNS


@pytest.mark.asyncio
async def test_resolve_table_feature() -> None:
    resolved = await _service().resolve_table(
        "compound@1.0.0", TableKind.FEATURE, feature_name=HookName("chem_features")
    )
    assert resolved.columns == FEATURE_COLUMNS


@pytest.mark.asyncio
async def test_resolve_table_unknown_feature_404s() -> None:
    with pytest.raises(NotFoundError) as exc:
        await _service().resolve_table(
            "compound@1.0.0", TableKind.FEATURE, feature_name=HookName("nope")
        )
    assert exc.value.code == "table_not_found"


@pytest.mark.asyncio
async def test_resolve_table_kind_must_match() -> None:
    # A resource named "records" exists, but with kind RECORDS — asking for a
    # FEATURE of that name must not match it.
    with pytest.raises(NotFoundError):
        await _service().resolve_table(
            "compound@1.0.0", TableKind.FEATURE, feature_name=HookName("records")
        )
