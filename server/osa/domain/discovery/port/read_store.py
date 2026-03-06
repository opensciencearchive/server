"""DiscoveryReadStore port — read-only access to records and feature data."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from osa.domain.discovery.model.value import (
        FeatureCatalogEntry,
        FeatureRow,
        Filter,
        RecordSummary,
        SortOrder,
    )
    from osa.domain.semantics.model.value import FieldType
    from osa.domain.shared.model.srn import RecordSRN


class DiscoveryReadStore(Protocol):
    async def search_records(
        self,
        filters: list[Filter],
        text_fields: list[str],
        q: str | None,
        sort: str,
        order: SortOrder,
        cursor: dict | None,
        limit: int,
        field_types: dict[str, FieldType] | None = None,
    ) -> tuple[list[RecordSummary], int]:
        """Search and filter published records. Returns (results, total_count)."""
        ...

    async def get_feature_catalog(self) -> list[FeatureCatalogEntry]:
        """List all feature tables with column schemas and record counts."""
        ...

    async def get_feature_table_schema(self, hook_name: str) -> FeatureCatalogEntry | None:
        """Look up a single feature table's schema by hook name.

        Returns None if the hook_name is not found.
        """
        ...

    async def search_features(
        self,
        hook_name: str,
        filters: list[Filter],
        record_srn: RecordSRN | None,
        sort: str,
        order: SortOrder,
        cursor: dict | None,
        limit: int,
    ) -> tuple[list[FeatureRow], int]:
        """Search and filter feature rows. Returns (rows, total_count)."""
        ...
