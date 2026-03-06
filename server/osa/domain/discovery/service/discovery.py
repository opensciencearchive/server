"""DiscoveryService — read-only business logic for record and feature search."""

from __future__ import annotations

import logging

from osa.domain.discovery.model.value import (
    VALID_OPERATORS,
    FeatureCatalog,
    FeatureSearchResult,
    Filter,
    FilterOperator,
    RecordSearchResult,
    SortOrder,
    decode_cursor,
    encode_cursor,
)
from osa.domain.discovery.port.field_definition_reader import FieldDefinitionReader
from osa.domain.discovery.port.read_store import DiscoveryReadStore
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import RecordSRN
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class DiscoveryService(Service):
    """Orchestrates validation and delegation for discovery queries."""

    read_store: DiscoveryReadStore
    field_reader: FieldDefinitionReader

    async def search_records(
        self,
        filters: list[Filter],
        q: str | None,
        sort: str,
        order: SortOrder,
        cursor: str | None,
        limit: int,
    ) -> RecordSearchResult:
        """Validate inputs and delegate record search to the read store."""
        if limit < 1 or limit > 100:
            raise ValidationError("limit must be between 1 and 100", field="limit")

        field_map = await self.field_reader.get_all_field_types()

        # Validate filter fields and operators
        for f in filters:
            if f.field not in field_map:
                raise ValidationError(
                    f"Unknown field '{f.field}': not defined in any registered schema",
                    field=f.field,
                )
            field_type = field_map[f.field]
            valid_ops = VALID_OPERATORS[field_type]
            if f.operator not in valid_ops:
                raise ValidationError(
                    f"Operator '{f.operator}' is not valid for field '{f.field}' "
                    f"(type '{field_type}'). Valid: {sorted(valid_ops)}",
                    field=f.field,
                )

        # Validate sort field
        if sort != "published_at" and sort not in field_map:
            raise ValidationError(
                f"Unknown sort field '{sort}': not defined in any registered schema",
                field="sort",
            )

        # Decode cursor
        decoded_cursor = None
        if cursor is not None:
            try:
                decoded_cursor = decode_cursor(cursor)
            except ValueError as exc:
                raise ValidationError(str(exc), field="cursor") from exc

        # Identify text-searchable fields for free-text q
        text_fields = [
            name for name, ft in field_map.items() if ft in (FieldType.TEXT, FieldType.URL)
        ]
        if q and not text_fields:
            raise ValidationError(
                "Free-text search is unavailable: no text or URL fields are registered",
                field="q",
            )

        results = await self.read_store.search_records(
            filters=filters,
            text_fields=text_fields,
            q=q,
            sort=sort,
            order=order,
            cursor=decoded_cursor,
            limit=limit,
            field_types=field_map,
        )

        has_more = len(results) == limit
        next_cursor = None
        if has_more and results:
            last = results[-1]
            if sort == "published_at":
                sort_val = last.published_at.isoformat()
            else:
                sort_val = last.metadata.get(sort)
            next_cursor = encode_cursor(sort_val, str(last.srn))

        return RecordSearchResult(
            results=results,
            cursor=next_cursor,
            has_more=has_more,
        )

    async def get_feature_catalog(self) -> FeatureCatalog:
        """Delegate feature catalog listing to the read store."""
        entries = await self.read_store.get_feature_catalog()
        return FeatureCatalog(tables=entries)

    async def search_features(
        self,
        hook_name: str,
        filters: list[Filter],
        record_srn: RecordSRN | None,
        sort: str,
        order: SortOrder,
        cursor: str | None,
        limit: int,
    ) -> FeatureSearchResult:
        """Validate inputs and delegate feature search to the read store."""
        if limit < 1 or limit > 100:
            raise ValidationError("limit must be between 1 and 100", field="limit")

        # Look up the feature table schema
        entry = await self.read_store.get_feature_table_schema(hook_name)
        if entry is None:
            raise NotFoundError(f"Feature table not found: {hook_name}")

        # Build column type map from catalog schema
        col_map: dict[str, str] = {col.name: col.type for col in entry.columns}
        # Also allow sort/filter on record_srn
        col_map["record_srn"] = "string"

        # Map JSON types to FieldType equivalents for operator validation
        json_type_to_ops: dict[str, set[FilterOperator]] = {
            "string": {FilterOperator.EQ, FilterOperator.CONTAINS},
            "number": {FilterOperator.EQ, FilterOperator.GTE, FilterOperator.LTE},
            "integer": {FilterOperator.EQ, FilterOperator.GTE, FilterOperator.LTE},
            "boolean": {FilterOperator.EQ},
            "array": {FilterOperator.EQ},
            "object": {FilterOperator.EQ},
        }

        # Validate filters
        for f in filters:
            if f.field not in col_map:
                raise ValidationError(
                    f"Unknown column '{f.field}' in feature table '{hook_name}'",
                    field=f.field,
                )
            json_type = col_map[f.field]
            valid_ops = json_type_to_ops.get(json_type, {FilterOperator.EQ})
            if f.operator not in valid_ops:
                raise ValidationError(
                    f"Operator '{f.operator}' is not valid for column '{f.field}' "
                    f"(type '{json_type}'). Valid: {sorted(valid_ops)}",
                    field=f.field,
                )

        # Validate sort column
        if sort != "id" and sort not in col_map:
            raise ValidationError(
                f"Unknown sort column '{sort}' in feature table '{hook_name}'",
                field="sort",
            )

        # Decode cursor
        try:
            decoded_cursor = decode_cursor(cursor) if cursor else None
        except ValueError as exc:
            raise ValidationError(str(exc), field="cursor") from exc

        rows = await self.read_store.search_features(
            hook_name=hook_name,
            filters=filters,
            record_srn=record_srn,
            sort=sort,
            order=order,
            cursor=decoded_cursor,
            limit=limit,
        )

        has_more = len(rows) == limit
        next_cursor = None
        if has_more and rows:
            last = rows[-1]
            if sort == "id":
                sort_val = last.row_id
            else:
                sort_val = last.data.get(sort)
            next_cursor = encode_cursor(sort_val, last.row_id)

        return FeatureSearchResult(
            rows=rows,
            cursor=next_cursor,
            has_more=has_more,
        )
