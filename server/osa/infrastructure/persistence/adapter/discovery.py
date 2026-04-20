"""Infrastructure adapters for the discovery domain — read-only SQL queries."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import date, datetime
from typing import Any

from sqlalchemy import (
    Date,
    Float,
    String,
    and_,
    cast,
    func,
    literal,
    not_,
    or_,
    select,
    true,
    union_all,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.discovery.model.refs import FeatureFieldRef, MetadataFieldRef
from osa.domain.discovery.model.value import (
    And,
    ColumnInfo,
    FeatureCatalogEntry,
    FeatureRow,
    FilterExpr,
    FilterOperator,
    Not,
    Or,
    Predicate,
    RecordSummary,
    SortOrder,
)
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import ConventionSRN, RecordSRN, SchemaSRN
from osa.infrastructure.persistence.feature_table import (
    FeatureSchema,
    build_feature_table,
    data_columns,
)
from osa.infrastructure.persistence.keyset import KeysetPage, SortKey
from osa.infrastructure.persistence.metadata_table import (
    MetadataSchema,
    build_metadata_table,
)
from osa.infrastructure.persistence.tables import (
    feature_tables_table,
    metadata_tables_table,
    records_table,
    schemas_table,
)

logger = logging.getLogger(__name__)


def _escape_like(value: str) -> str:
    """Escape LIKE metacharacters so user input is matched literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# Cursor-value coercers — cursor payloads round-trip through base64 JSON as
# plain strings/numbers, but keyset predicates compare against typed columns.
# Without this, ``published_at < 'iso-string'::VARCHAR`` fails on Postgres.

CursorCoercer = Callable[[Any], Any]


def _coerce_identity(value: Any) -> Any:
    return value


def _coerce_datetime(value: Any) -> Any:
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def _coerce_date(value: Any) -> Any:
    if isinstance(value, str):
        return date.fromisoformat(value)
    return value


def _coerce_float(value: Any) -> Any:
    return None if value is None else float(value)


def _coerce_int(value: Any) -> Any:
    return None if value is None else int(value)


def _coercer_for_column(col_def: ColumnDef) -> CursorCoercer:
    """Pick a coercer matching the Postgres type chosen by ``column_mapper``."""
    if col_def.json_type == "number":
        return _coerce_float
    if col_def.json_type == "integer":
        return _coerce_int
    if col_def.json_type == "string":
        if col_def.format == "date-time":
            return _coerce_datetime
        if col_def.format == "date":
            return _coerce_date
    return _coerce_identity


def _to_column_info(columns: list[Any]) -> list[ColumnInfo]:
    return [ColumnInfo(name=c.name, type=c.json_type, required=c.required) for c in columns]


class PostgresFieldDefinitionReader:
    """Builds field name → FieldType maps from registered schemas."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_all_field_types(self) -> dict[str, FieldType]:
        stmt = select(schemas_table.c.srn, schemas_table.c.fields)
        result = await self.session.execute(stmt)
        rows = result.mappings().all()

        field_map: dict[str, FieldType] = {}
        for row in rows:
            for field_def in row["fields"]:
                name = field_def["name"]
                field_type = FieldType(field_def["type"])
                if name in field_map and field_map[name] != field_type:
                    raise ValidationError(
                        f"Conflicting types for field '{name}': "
                        f"'{field_map[name]}' vs '{field_type}'",
                        field=name,
                    )
                field_map[name] = field_type

        return field_map

    async def get_fields_for_schema(self, schema_srn: SchemaSRN) -> dict[str, FieldType]:
        rendered = str(schema_srn)
        stmt = select(schemas_table.c.fields).where(schemas_table.c.srn == rendered)
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return {}
        return {f["name"]: FieldType(f["type"]) for f in row["fields"]}


class PostgresDiscoveryReadStore:
    """Compiles FilterExpr trees into SQLAlchemy queries over records / metadata / features."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def search_records(
        self,
        filter_expr: FilterExpr | None,
        schema_srn: SchemaSRN | None,
        convention_srn: ConventionSRN | None,
        text_fields: list[str],
        q: str | None,
        sort: str,
        order: SortOrder,
        cursor: dict[str, Any] | None,
        limit: int,
        field_types: dict[str, FieldType] | None = None,
    ) -> list[RecordSummary]:
        t = records_table
        ft_map = field_types or {}

        metadata_table = None
        metadata_schema: MetadataSchema | None = None
        if schema_srn is not None:
            catalog = await self._metadata_catalog_for(schema_srn)
            if catalog is not None:
                metadata_schema = MetadataSchema.model_validate(catalog["metadata_schema"])
                metadata_table = build_metadata_table(catalog["pg_table"], metadata_schema)

        feature_joins = await self._collect_feature_joins(filter_expr)

        conditions: list[Any] = []

        if convention_srn is not None:
            conditions.append(t.c.convention_srn == str(convention_srn))

        if filter_expr is not None:
            conditions.append(
                self._compile_filter_for_records(
                    filter_expr,
                    records_t=t,
                    metadata_t=metadata_table,
                    metadata_schema=metadata_schema,
                    feature_joins=feature_joins,
                    field_types=ft_map,
                )
            )

        if q and text_fields and metadata_table is not None and metadata_schema is not None:
            pattern = f"%{_escape_like(q)}%"
            text_col_names = {c.name for c in metadata_schema.columns if c.json_type == "string"}
            text_clauses = [
                cast(metadata_table.c[name], String).ilike(pattern, escape="\\")
                for name in text_fields
                if name in text_col_names
            ]
            if text_clauses:
                conditions.append(or_(*text_clauses))

        # Sort expression + matching cursor-value coercer
        if sort == "published_at":
            sort_expr = t.c.published_at
            coerce_cursor: CursorCoercer = _coerce_datetime
        elif metadata_table is not None and sort in metadata_table.c:
            col = metadata_table.c[sort]
            if ft_map.get(sort) == FieldType.NUMBER:
                sort_expr = cast(col, Float)
                coerce_cursor = _coerce_float
            elif ft_map.get(sort) == FieldType.DATE:
                sort_expr = cast(col, Date)
                coerce_cursor = _coerce_date
            else:
                sort_expr = col
                coerce_cursor = _coerce_identity
        else:
            sort_expr = t.c.published_at
            coerce_cursor = _coerce_datetime

        is_desc = order == SortOrder.DESC
        page = KeysetPage(
            [
                SortKey(sort_expr, descending=is_desc, nulls_last=True),
                SortKey(t.c.srn, descending=is_desc),
            ]
        )
        order_clauses = page.order_by()
        if cursor is not None:
            sort_value = coerce_cursor(cursor["s"])
            conditions.append(page.after((sort_value, cursor["id"])))

        where_clause = and_(*conditions) if conditions else true()

        if metadata_table is not None and metadata_schema is not None:
            select_cols = [t.c.srn, t.c.published_at] + [
                metadata_table.c[c.name].label(c.name) for c in metadata_schema.columns
            ]
            stmt = select(*select_cols).select_from(
                t.join(metadata_table, metadata_table.c.record_srn == t.c.srn)
            )
        else:
            # No schema pinned — project the canonical JSONB metadata column.
            # Typed tables are a query-optimized projection; JSONB remains the
            # authoritative source for presentation (and for cross-schema
            # listings where no single typed table applies).
            stmt = select(t.c.srn, t.c.published_at, t.c.metadata)

        for hook, ft in feature_joins.items():
            stmt = stmt.join(ft, ft.c.record_srn == t.c.srn, isouter=True)

        stmt = stmt.where(where_clause).order_by(*order_clauses).limit(limit)

        result = await self.session.execute(stmt)
        summaries: list[RecordSummary] = []
        if metadata_table is not None and metadata_schema is not None:
            for row in result.mappings():
                meta = {c.name: row[c.name] for c in metadata_schema.columns if c.name in row}
                summaries.append(
                    RecordSummary(
                        srn=RecordSRN.parse(row["srn"]),
                        published_at=row["published_at"],
                        metadata=meta,
                    )
                )
        else:
            for row in result.mappings():
                summaries.append(
                    RecordSummary(
                        srn=RecordSRN.parse(row["srn"]),
                        published_at=row["published_at"],
                        metadata=row.get("metadata") or {},
                    )
                )
        return summaries

    async def get_feature_catalog(self) -> list[FeatureCatalogEntry]:
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.pg_table,
            feature_tables_table.c.feature_schema,
        )
        result = await self.session.execute(stmt)
        catalog_rows = result.mappings().all()

        if not catalog_rows:
            return []

        parsed = [
            (row["hook_name"], FeatureSchema.model_validate(row["feature_schema"]), row["pg_table"])
            for row in catalog_rows
        ]

        count_parts = []
        for hook_name, schema, pg_table in parsed:
            ft = build_feature_table(pg_table, schema)
            count_parts.append(
                select(
                    literal(hook_name).label("hook_name"),
                    func.count(func.distinct(ft.c.record_srn)).label("cnt"),
                ).select_from(ft)
            )
        counts_result = await self.session.execute(union_all(*count_parts))
        counts_by_hook = {r["hook_name"]: r["cnt"] for r in counts_result.mappings()}

        return [
            FeatureCatalogEntry(
                hook_name=hook_name,
                columns=_to_column_info(schema.columns),
                record_count=counts_by_hook.get(hook_name, 0),
            )
            for hook_name, schema, _pg_table in parsed
        ]

    async def get_feature_table_schema(self, hook_name: str) -> FeatureCatalogEntry | None:
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.feature_schema,
        ).where(feature_tables_table.c.hook_name == hook_name)
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None

        schema = FeatureSchema.model_validate(row["feature_schema"])
        return FeatureCatalogEntry(
            hook_name=row["hook_name"],
            columns=_to_column_info(schema.columns),
            record_count=0,
        )

    async def search_features(
        self,
        hook_name: str,
        filter_expr: FilterExpr | None,
        schema_srn: SchemaSRN | None,
        record_srn: RecordSRN | None,
        sort: str,
        order: SortOrder,
        cursor: dict[str, Any] | None,
        limit: int,
    ) -> list[FeatureRow]:
        pg_table_stmt = select(
            feature_tables_table.c.pg_table,
            feature_tables_table.c.feature_schema,
        ).where(feature_tables_table.c.hook_name == hook_name)
        pg_result = await self.session.execute(pg_table_stmt)
        pg_row = pg_result.mappings().first()
        if pg_row is None:
            return []
        pg_table: str = pg_row["pg_table"]
        schema = FeatureSchema.model_validate(pg_row["feature_schema"])

        ft = build_feature_table(pg_table, schema)

        metadata_table = None
        metadata_schema: MetadataSchema | None = None
        if schema_srn is not None:
            catalog = await self._metadata_catalog_for(schema_srn)
            if catalog is not None:
                metadata_schema = MetadataSchema.model_validate(catalog["metadata_schema"])
                metadata_table = build_metadata_table(catalog["pg_table"], metadata_schema)

        feature_joins: dict[str, Any] = {}
        if filter_expr is not None:
            extra = await self._collect_feature_joins(filter_expr)
            for hook, tbl in extra.items():
                if hook != hook_name:
                    feature_joins[hook] = tbl

        conditions: list[Any] = []

        if record_srn is not None:
            conditions.append(ft.c.record_srn == str(record_srn))

        if filter_expr is not None:
            conditions.append(
                self._compile_filter_for_features(
                    filter_expr,
                    this_hook=hook_name,
                    this_ft=ft,
                    metadata_t=metadata_table,
                    metadata_schema=metadata_schema,
                    feature_joins=feature_joins,
                )
            )

        if sort == "id":
            sort_expr = ft.c.id
            coerce_cursor: CursorCoercer = _coerce_int
        else:
            sort_expr = ft.c[sort]
            col_def = next((c for c in schema.columns if c.name == sort), None)
            coerce_cursor = (
                _coercer_for_column(col_def) if col_def is not None else _coerce_identity
            )

        is_desc = order == SortOrder.DESC
        page = KeysetPage(
            [
                SortKey(sort_expr, descending=is_desc, nulls_last=True),
                SortKey(ft.c.id, descending=is_desc),
            ]
        )
        order_clauses = page.order_by()
        if cursor is not None:
            sort_value = coerce_cursor(cursor["s"])
            conditions.append(page.after((sort_value, cursor["id"])))

        where_clause = and_(*conditions) if conditions else true()

        stmt = select(ft.c.id, ft.c.record_srn, *data_columns(ft))
        select_from = ft
        if metadata_table is not None:
            select_from = select_from.join(
                metadata_table, metadata_table.c.record_srn == ft.c.record_srn, isouter=True
            )
        for hook, other_ft in feature_joins.items():
            select_from = select_from.join(
                other_ft, other_ft.c.record_srn == ft.c.record_srn, isouter=True
            )
        stmt = (
            stmt.select_from(select_from).where(where_clause).order_by(*order_clauses).limit(limit)
        )

        result = await self.session.execute(stmt)
        feature_rows: list[FeatureRow] = []
        for row in result.mappings():
            row_dict = dict(row)
            row_id = row_dict.pop("id")
            rsrn = RecordSRN.parse(row_dict.pop("record_srn"))
            feature_rows.append(FeatureRow(row_id=row_id, record_srn=rsrn, data=row_dict))

        return feature_rows

    # ---------------- compilation helpers ----------------

    async def _metadata_catalog_for(self, schema_srn: SchemaSRN) -> dict[str, Any] | None:
        """Look up the metadata table catalog row for a Schema SRN."""
        identity = str(schema_srn).split("@", 1)[0]
        major = int(schema_srn.version.root.split(".")[0])
        stmt = select(metadata_tables_table).where(
            metadata_tables_table.c.schema_identity == identity,
            metadata_tables_table.c.schema_major == major,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def _collect_feature_joins(self, filter_expr: FilterExpr | None) -> dict[str, Any]:
        """Build {hook_name: SQLA Table} for every distinct feature ref in the tree."""
        if filter_expr is None:
            return {}
        hooks: set[str] = set()
        for p in _iter_predicates(filter_expr):
            if isinstance(p.field, FeatureFieldRef):
                hooks.add(p.field.hook)
        if not hooks:
            return {}
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.pg_table,
            feature_tables_table.c.feature_schema,
        ).where(feature_tables_table.c.hook_name.in_(hooks))
        result = await self.session.execute(stmt)
        joins: dict[str, Any] = {}
        for row in result.mappings():
            schema = FeatureSchema.model_validate(row["feature_schema"])
            joins[row["hook_name"]] = build_feature_table(row["pg_table"], schema)
        missing = hooks - joins.keys()
        if missing:
            raise ValidationError(
                f"Unknown feature hook(s): {sorted(missing)}",
                field="filter",
                code="unknown_hook",
            )
        return joins

    def _compile_filter_for_records(
        self,
        expr: FilterExpr,
        *,
        records_t: Any,
        metadata_t: Any,
        metadata_schema: MetadataSchema | None,
        feature_joins: dict[str, Any],
        field_types: dict[str, FieldType],
    ) -> Any:
        if isinstance(expr, Predicate):
            return self._compile_predicate(
                expr,
                metadata_t=metadata_t,
                metadata_schema=metadata_schema,
                feature_joins=feature_joins,
                field_types=field_types,
            )
        if isinstance(expr, And):
            return and_(
                *[
                    self._compile_filter_for_records(
                        op,
                        records_t=records_t,
                        metadata_t=metadata_t,
                        metadata_schema=metadata_schema,
                        feature_joins=feature_joins,
                        field_types=field_types,
                    )
                    for op in expr.operands
                ]
            )
        if isinstance(expr, Or):
            return or_(
                *[
                    self._compile_filter_for_records(
                        op,
                        records_t=records_t,
                        metadata_t=metadata_t,
                        metadata_schema=metadata_schema,
                        feature_joins=feature_joins,
                        field_types=field_types,
                    )
                    for op in expr.operands
                ]
            )
        if isinstance(expr, Not):
            return not_(
                self._compile_filter_for_records(
                    expr.operand,
                    records_t=records_t,
                    metadata_t=metadata_t,
                    metadata_schema=metadata_schema,
                    feature_joins=feature_joins,
                    field_types=field_types,
                )
            )
        raise ValidationError(f"Unsupported filter node: {type(expr).__name__}")

    def _compile_filter_for_features(
        self,
        expr: FilterExpr,
        *,
        this_hook: str,
        this_ft: Any,
        metadata_t: Any,
        metadata_schema: MetadataSchema | None,
        feature_joins: dict[str, Any],
    ) -> Any:
        if isinstance(expr, Predicate):
            if isinstance(expr.field, MetadataFieldRef):
                if metadata_t is None:
                    raise ValidationError(
                        f"Metadata ref {expr.field.dotted()!r} requires schema_srn to be set.",
                        field=expr.field.dotted(),
                        code="metadata_ref_requires_schema",
                    )
                col = metadata_t.c[expr.field.field]
                return _apply_scalar_op(col, expr.op, expr.value)
            assert isinstance(expr.field, FeatureFieldRef)
            if expr.field.hook == this_hook:
                col = this_ft.c[expr.field.column]
            else:
                tbl = feature_joins.get(expr.field.hook)
                if tbl is None:
                    raise ValidationError(
                        f"Unknown feature hook '{expr.field.hook}'.",
                        field=expr.field.dotted(),
                        code="unknown_hook",
                    )
                col = tbl.c[expr.field.column]
            return _apply_scalar_op(col, expr.op, expr.value)
        if isinstance(expr, And):
            return and_(
                *[
                    self._compile_filter_for_features(
                        op,
                        this_hook=this_hook,
                        this_ft=this_ft,
                        metadata_t=metadata_t,
                        metadata_schema=metadata_schema,
                        feature_joins=feature_joins,
                    )
                    for op in expr.operands
                ]
            )
        if isinstance(expr, Or):
            return or_(
                *[
                    self._compile_filter_for_features(
                        op,
                        this_hook=this_hook,
                        this_ft=this_ft,
                        metadata_t=metadata_t,
                        metadata_schema=metadata_schema,
                        feature_joins=feature_joins,
                    )
                    for op in expr.operands
                ]
            )
        if isinstance(expr, Not):
            return not_(
                self._compile_filter_for_features(
                    expr.operand,
                    this_hook=this_hook,
                    this_ft=this_ft,
                    metadata_t=metadata_t,
                    metadata_schema=metadata_schema,
                    feature_joins=feature_joins,
                )
            )
        raise ValidationError(f"Unsupported filter node: {type(expr).__name__}")

    def _compile_predicate(
        self,
        predicate: Predicate,
        *,
        metadata_t: Any,
        metadata_schema: MetadataSchema | None,
        feature_joins: dict[str, Any],
        field_types: dict[str, FieldType],
    ) -> Any:
        if isinstance(predicate.field, MetadataFieldRef):
            # Prefer the typed projection when a schema is pinned.
            if metadata_t is not None and metadata_schema is not None:
                col = metadata_t.c[predicate.field.field]
                return _apply_scalar_op(col, predicate.op, predicate.value)
            # Otherwise compile against the canonical records.metadata JSONB.
            return _apply_jsonb_op(
                records_table,
                field=predicate.field.field,
                op=predicate.op,
                value=predicate.value,
                field_type=field_types.get(predicate.field.field),
            )

        assert isinstance(predicate.field, FeatureFieldRef)
        tbl = feature_joins.get(predicate.field.hook)
        if tbl is None:
            raise ValidationError(
                f"Unknown feature hook '{predicate.field.hook}'.",
                field=predicate.field.dotted(),
                code="unknown_hook",
            )
        col = tbl.c[predicate.field.column]
        return _apply_scalar_op(col, predicate.op, predicate.value)


def _apply_jsonb_op(
    records_t: Any,
    *,
    field: str,
    op: FilterOperator,
    value: Any,
    field_type: FieldType | None,
) -> Any:
    """Compile a metadata-field predicate against the canonical ``records.metadata`` JSONB.

    Used when no ``schema_srn`` is pinned (cross-schema / unscoped listings).
    Equality uses JSONB containment (GIN-indexed); range ops cast the extracted
    text to the appropriate type driven by ``field_type`` when known.
    """
    meta = records_t.c.metadata

    if op == FilterOperator.EQ:
        return meta.op("@>")(cast(func.json_build_object(field, value), JSONB))
    if op == FilterOperator.NEQ:
        return not_(meta.op("@>")(cast(func.json_build_object(field, value), JSONB)))
    if op == FilterOperator.IS_NULL:
        # Absent key OR present-but-null both count as "null".
        return or_(not_(meta.has_key(field)), meta[field].astext.is_(None))
    if op == FilterOperator.IN:
        if not isinstance(value, list):
            raise ValidationError(
                "Operator 'in' requires a list value.",
                field=field,
                code="invalid_value_for_op",
            )
        return meta[field].astext.in_([str(v) for v in value])
    if op == FilterOperator.CONTAINS:
        return meta[field].astext.ilike(f"%{_escape_like(str(value))}%", escape="\\")
    if op in (FilterOperator.GT, FilterOperator.GTE, FilterOperator.LT, FilterOperator.LTE):
        if field_type == FieldType.NUMBER:
            col_expr = cast(meta[field].astext, Float)
            typed_value: Any = float(value)
        elif field_type == FieldType.DATE:
            col_expr = cast(meta[field].astext, Date)
            typed_value = str(value)
        else:
            col_expr = cast(meta[field].astext, String)
            typed_value = str(value)
        if op == FilterOperator.GT:
            return col_expr > typed_value
        if op == FilterOperator.GTE:
            return col_expr >= typed_value
        if op == FilterOperator.LT:
            return col_expr < typed_value
        return col_expr <= typed_value
    raise ValidationError(
        f"Unsupported operator for JSONB fallback: {op}",
        field=field,
        code="unsupported_operator",
    )


def _apply_scalar_op(col: Any, op: FilterOperator, value: Any) -> Any:
    if op == FilterOperator.EQ:
        return col == value
    if op == FilterOperator.NEQ:
        return col != value
    if op == FilterOperator.GT:
        return col > value
    if op == FilterOperator.GTE:
        return col >= value
    if op == FilterOperator.LT:
        return col < value
    if op == FilterOperator.LTE:
        return col <= value
    if op == FilterOperator.IN:
        if not isinstance(value, list):
            raise ValidationError(
                "Operator 'in' requires a list value.",
                field=col.key,
                code="invalid_value_for_op",
            )
        return col.in_(value)
    if op == FilterOperator.CONTAINS:
        return cast(col, String).ilike(f"%{_escape_like(str(value))}%", escape="\\")
    if op == FilterOperator.IS_NULL:
        return col.is_(None)
    raise ValidationError(
        f"Unsupported operator: {op}", field="filter", code="unsupported_operator"
    )


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
