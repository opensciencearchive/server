"""DiscoveryService — read-only business logic for record and feature search.

Validates the compound ``FilterExpr`` tree (bounds, field resolution, operator
compatibility) before handing it to the read store for SQL compilation.
"""

from __future__ import annotations

import logging
from typing import Any

from osa.config import Config
from osa.domain.discovery.model.refs import (
    FeatureFieldRef,
    MetadataFieldRef,
)
from osa.domain.discovery.model.value import (
    JSON_TYPE_OPERATORS,
    VALID_OPERATORS,
    And,
    FeatureCatalog,
    FeatureSearchResult,
    FilterExpr,
    FilterOperator,
    Not,
    Or,
    Predicate,
    RecordSearchResult,
    SortOrder,
    decode_cursor,
    encode_cursor,
)
from osa.domain.discovery.port.field_definition_reader import FieldDefinitionReader
from osa.domain.discovery.port.read_store import DiscoveryReadStore
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN, SchemaSRN
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class DiscoveryService(Service):
    """Orchestrates validation and delegation for discovery queries."""

    read_store: DiscoveryReadStore
    field_reader: FieldDefinitionReader
    config: Config

    async def search_records(
        self,
        filter_expr: FilterExpr | None,
        schema_srn: SchemaSRN | None,
        convention_srn: ConventionSRN | None,
        q: str | None,
        sort: str,
        order: SortOrder,
        cursor: str | None,
        limit: int,
        *,
        allow_compound: bool = True,
    ) -> RecordSearchResult:
        """Validate the filter tree and delegate record search to the read store.

        ``allow_compound`` is a staged flag — US1 delivers AND-only + Predicate
        support; US2 flips this to allow OR/NOT. Callers should leave it True
        once US2 lands.
        """
        if limit < 1 or limit > 100:
            raise ValidationError("limit must be between 1 and 100", field="limit")

        schema_field_map: dict[str, FieldType] = {}
        if schema_srn is not None:
            schema_field_map = await self.field_reader.get_fields_for_schema(schema_srn)

        global_field_map = await self.field_reader.get_all_field_types()
        effective_field_map = schema_field_map or global_field_map

        if filter_expr is not None:
            self._validate_tree(filter_expr, allow_compound=allow_compound)
            await self._validate_refs(filter_expr, schema_srn, effective_field_map)

        # Sort field validation
        if sort != "published_at" and sort not in effective_field_map:
            raise ValidationError(
                f"Unknown sort field '{sort}': not defined in registered schema",
                field="sort",
            )

        decoded_cursor: dict[str, Any] | None = None
        if cursor is not None:
            try:
                decoded_cursor = decode_cursor(cursor)
            except ValueError as exc:
                raise ValidationError(str(exc), field="cursor") from exc

        text_fields = [
            name
            for name, ft in effective_field_map.items()
            if ft in (FieldType.TEXT, FieldType.URL)
        ]
        if q and not text_fields:
            raise ValidationError(
                "Free-text search is unavailable: no text or URL fields are registered",
                field="q",
            )

        results = await self.read_store.search_records(
            filter_expr=filter_expr,
            schema_srn=schema_srn,
            convention_srn=convention_srn,
            text_fields=text_fields,
            q=q,
            sort=sort,
            order=order,
            cursor=decoded_cursor,
            limit=limit + 1,
            field_types=effective_field_map,
        )

        has_more = len(results) > limit
        results = results[:limit]
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
        entries = await self.read_store.get_feature_catalog()
        return FeatureCatalog(tables=entries)

    async def search_features(
        self,
        hook_name: str,
        filter_expr: FilterExpr | None,
        schema_srn: SchemaSRN | None,
        record_srn: RecordSRN | None,
        sort: str,
        order: SortOrder,
        cursor: str | None,
        limit: int,
        *,
        allow_compound: bool = True,
    ) -> FeatureSearchResult:
        if limit < 1 or limit > 100:
            raise ValidationError("limit must be between 1 and 100", field="limit")

        entry = await self.read_store.get_feature_table_schema(hook_name)
        if entry is None:
            raise NotFoundError(f"Feature table not found: {hook_name}")

        col_map: dict[str, str] = {col.name: col.type for col in entry.columns}
        col_map["record_srn"] = "string"

        schema_field_map: dict[str, FieldType] = {}
        if schema_srn is not None:
            schema_field_map = await self.field_reader.get_fields_for_schema(schema_srn)

        if filter_expr is not None:
            self._validate_tree(filter_expr, allow_compound=allow_compound)
            self._validate_feature_refs(
                filter_expr,
                this_hook=hook_name,
                feature_col_map=col_map,
                schema_field_map=schema_field_map,
            )

        if sort != "id" and sort not in col_map:
            raise ValidationError(
                f"Unknown sort column '{sort}' in feature table '{hook_name}'",
                field="sort",
            )

        try:
            decoded_cursor = decode_cursor(cursor) if cursor else None
        except ValueError as exc:
            raise ValidationError(str(exc), field="cursor") from exc

        rows = await self.read_store.search_features(
            hook_name=hook_name,
            filter_expr=filter_expr,
            schema_srn=schema_srn,
            record_srn=record_srn,
            sort=sort,
            order=order,
            cursor=decoded_cursor,
            limit=limit + 1,
        )

        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = None
        if has_more and rows:
            last = rows[-1]
            if sort == "id":
                sort_val = last.row_id
            else:
                sort_val = last.data.get(sort)
            next_cursor = encode_cursor(sort_val, last.row_id)

        return FeatureSearchResult(rows=rows, cursor=next_cursor, has_more=has_more)

    # ------------------------- internal helpers -------------------------

    def _validate_tree(self, expr: FilterExpr, *, allow_compound: bool) -> None:
        """Enforce tree bounds (depth, predicate count, joins) + compound gating."""
        depth = _tree_depth(expr)
        predicates = list(_iter_predicates(expr))

        if depth > self.config.discovery_max_filter_depth:
            raise ValidationError(
                f"Filter tree depth {depth} exceeds configured maximum "
                f"{self.config.discovery_max_filter_depth} (OSA_DISCOVERY_MAX_FILTER_DEPTH).",
                field="filter",
                code="filter_depth_exceeded",
            )
        if len(predicates) > self.config.discovery_max_predicates:
            raise ValidationError(
                f"Filter tree has {len(predicates)} predicate leaves, exceeds "
                f"configured maximum {self.config.discovery_max_predicates} "
                "(OSA_DISCOVERY_MAX_PREDICATES).",
                field="filter",
                code="filter_predicates_exceeded",
            )

        distinct_hooks: set[str] = set()
        for p in predicates:
            if isinstance(p.field, FeatureFieldRef):
                distinct_hooks.add(p.field.hook)
        if len(distinct_hooks) > self.config.discovery_max_cross_domain_joins:
            raise ValidationError(
                f"Filter tree joins {len(distinct_hooks)} distinct feature hooks, "
                f"exceeds configured maximum "
                f"{self.config.discovery_max_cross_domain_joins} "
                "(OSA_DISCOVERY_MAX_CROSS_DOMAIN_JOINS).",
                field="filter",
                code="filter_joins_exceeded",
            )

        if not allow_compound:
            for node in _iter_nodes(expr):
                if isinstance(node, (Or, Not)):
                    raise ValidationError(
                        "Compound OR/NOT filters are not enabled in this build.",
                        field="filter",
                        code="compound_disabled",
                    )

    async def _validate_refs(
        self,
        expr: FilterExpr,
        schema_srn: SchemaSRN | None,
        field_map: dict[str, FieldType],
    ) -> None:
        """Resolve each predicate's field and check operator compatibility."""
        feature_catalog: dict[str, dict[str, str]] | None = None
        for p in _iter_predicates(expr):
            if isinstance(p.field, MetadataFieldRef):
                if schema_srn is None and not field_map:
                    raise ValidationError(
                        f"Unknown metadata field '{p.field.field}': "
                        "no schema_srn provided and no registered schemas.",
                        field=p.field.dotted(),
                        code="unknown_field",
                    )
                field_name = p.field.field
                if field_name not in field_map:
                    raise ValidationError(
                        f"Unknown metadata field '{field_name}' for the provided schema.",
                        field=p.field.dotted(),
                        code="unknown_field",
                    )
                self._check_operator_for_field_type(
                    p, field_type=field_map[field_name], path=p.field.dotted()
                )
            elif isinstance(p.field, FeatureFieldRef):
                if feature_catalog is None:
                    feature_catalog = await self._load_feature_catalog()
                cols = feature_catalog.get(p.field.hook)
                if cols is None:
                    raise ValidationError(
                        f"Unknown feature hook '{p.field.hook}'.",
                        field=p.field.dotted(),
                        code="unknown_hook",
                    )
                if p.field.column not in cols:
                    raise ValidationError(
                        f"Unknown feature column '{p.field.column}' on hook '{p.field.hook}'.",
                        field=p.field.dotted(),
                        code="unknown_field",
                    )
                json_type = cols[p.field.column]
                self._check_operator_for_json_type(p, json_type=json_type, path=p.field.dotted())

    def _validate_feature_refs(
        self,
        expr: FilterExpr,
        *,
        this_hook: str,
        feature_col_map: dict[str, str],
        schema_field_map: dict[str, FieldType],
    ) -> None:
        """Variant of ref validation for feature search — local hook columns by default."""
        for p in _iter_predicates(expr):
            if isinstance(p.field, MetadataFieldRef):
                if p.field.field not in schema_field_map:
                    raise ValidationError(
                        f"Unknown metadata field '{p.field.field}' for the provided schema.",
                        field=p.field.dotted(),
                        code="unknown_field",
                    )
                self._check_operator_for_field_type(
                    p, field_type=schema_field_map[p.field.field], path=p.field.dotted()
                )
            elif isinstance(p.field, FeatureFieldRef):
                if p.field.hook != this_hook:
                    # Cross-hook joins handled by US3 — accepted here, resolved in adapter.
                    continue
                if p.field.column not in feature_col_map:
                    raise ValidationError(
                        f"Unknown feature column '{p.field.column}' on hook '{this_hook}'.",
                        field=p.field.dotted(),
                        code="unknown_field",
                    )
                self._check_operator_for_json_type(
                    p, json_type=feature_col_map[p.field.column], path=p.field.dotted()
                )

    async def _load_feature_catalog(self) -> dict[str, dict[str, str]]:
        """Build hook_name → {column_name → json_type} map from the catalog."""
        catalog = await self.read_store.get_feature_catalog()
        return {entry.hook_name: {col.name: col.type for col in entry.columns} for entry in catalog}

    @staticmethod
    def _check_operator_for_field_type(
        predicate: Predicate, *, field_type: FieldType, path: str
    ) -> None:
        valid = VALID_OPERATORS.get(field_type, set())
        if predicate.op not in valid:
            raise ValidationError(
                f"Operator '{predicate.op}' is not valid for field '{path}' "
                f"(type '{field_type}'). Valid: {sorted(valid)}.",
                field=path,
                code="operator_not_valid_for_type",
            )

    @staticmethod
    def _check_operator_for_json_type(predicate: Predicate, *, json_type: str, path: str) -> None:
        valid = JSON_TYPE_OPERATORS.get(json_type, {FilterOperator.EQ})
        if predicate.op not in valid:
            raise ValidationError(
                f"Operator '{predicate.op}' is not valid for column '{path}' "
                f"(json type '{json_type}'). Valid: {sorted(valid)}.",
                field=path,
                code="operator_not_valid_for_type",
            )


def _tree_depth(expr: FilterExpr) -> int:
    if isinstance(expr, Predicate):
        return 1
    if isinstance(expr, Not):
        return 1 + _tree_depth(expr.operand)
    if isinstance(expr, (And, Or)):
        return 1 + max(_tree_depth(op) for op in expr.operands)
    return 1


def _iter_predicates(expr: FilterExpr):
    if isinstance(expr, Predicate):
        yield expr
        return
    if isinstance(expr, Not):
        yield from _iter_predicates(expr.operand)
        return
    if isinstance(expr, (And, Or)):
        for op in expr.operands:
            yield from _iter_predicates(op)


def _iter_nodes(expr: FilterExpr):
    yield expr
    if isinstance(expr, Not):
        yield from _iter_nodes(expr.operand)
    elif isinstance(expr, (And, Or)):
        for op in expr.operands:
            yield from _iter_nodes(op)
