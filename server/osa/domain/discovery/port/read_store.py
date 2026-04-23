"""DiscoveryReadStore port — read-only access to records, features, metadata."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from osa.domain.discovery.model.value import (
        FeatureCatalogEntry,
        FeatureRow,
        FilterExpr,
        RecordSummary,
        SortOrder,
    )
    from osa.domain.semantics.model.value import FieldType
    from osa.domain.shared.model.srn import ConventionSRN, RecordSRN, SchemaId


class DiscoveryReadStore(Protocol):
    async def search_records(
        self,
        filter_expr: "FilterExpr | None",
        schema_id: "SchemaId | None",
        convention_srn: "ConventionSRN | None",
        text_fields: list[str],
        q: str | None,
        sort: str,
        order: "SortOrder",
        cursor: dict | None,
        limit: int,
        field_types: "dict[str, FieldType] | None" = None,
    ) -> "list[RecordSummary]":
        """Search published records with a compound filter."""
        ...

    async def get_feature_catalog(self) -> "list[FeatureCatalogEntry]": ...

    async def get_feature_table_schema(self, hook_name: str) -> "FeatureCatalogEntry | None": ...

    async def search_features(
        self,
        hook_name: str,
        filter_expr: "FilterExpr | None",
        schema_id: "SchemaId | None",
        record_srn: "RecordSRN | None",
        sort: str,
        order: "SortOrder",
        cursor: dict | None,
        limit: int,
    ) -> "list[FeatureRow]": ...
