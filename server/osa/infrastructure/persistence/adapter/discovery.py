"""Infrastructure adapters for the discovery domain — read-only SQL queries."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import (
    Column,
    Date,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    and_,
    cast,
    func,
    literal,
    or_,
    select,
    text,
    union_all,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import quoted_name

from osa.domain.discovery.model.value import (
    ColumnInfo,
    FeatureCatalogEntry,
    FeatureRow,
    Filter,
    FilterOperator,
    RecordSummary,
    SortOrder,
)
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.persistence.tables import (
    feature_tables_table,
    records_table,
    schemas_table,
)

logger = logging.getLogger(__name__)


def _escape_like(value: str) -> str:
    """Escape LIKE metacharacters so user input is matched literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


class PostgresFieldDefinitionReader:
    """Builds a global field_name -> FieldType map from all registered schemas."""

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


class PostgresDiscoveryReadStore:
    """Direct SQL queries against records and feature tables for discovery."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def search_records(
        self,
        filters: list[Filter],
        text_fields: list[str],
        q: str | None,
        sort: str,
        order: SortOrder,
        cursor: dict[str, Any] | None,
        limit: int,
        field_types: dict[str, FieldType] | None = None,
    ) -> list[RecordSummary]:
        """Build and execute a dynamic SQL query for record search."""
        t = records_table
        conditions: list[Any] = []
        ft = field_types or {}

        # Build filter conditions
        for f in filters:
            conditions.append(self._record_filter_clause(f, ft.get(f.field)))

        # Free-text search across text fields
        if q and text_fields:
            pattern = f"%{_escape_like(q)}%"
            text_clauses = [
                t.c.metadata[field].astext.ilike(pattern, escape="\\") for field in text_fields
            ]
            conditions.append(or_(*text_clauses))

        # Determine sort expression (cast to match field type for correct ordering)
        if sort == "published_at":
            sort_expr = t.c.published_at
        elif ft.get(sort) == FieldType.NUMBER:
            sort_expr = cast(t.c.metadata[sort].astext, Float)
        elif ft.get(sort) == FieldType.DATE:
            sort_expr = cast(t.c.metadata[sort].astext, Date)
        else:
            sort_expr = t.c.metadata[sort].astext

        # Sort direction
        if order == SortOrder.ASC:
            order_clauses = [sort_expr.asc().nullslast(), t.c.srn.asc()]
        else:
            order_clauses = [sort_expr.desc().nullslast(), t.c.srn.desc()]

        # Keyset cursor
        if cursor is not None:
            cursor_sort = cursor["s"]
            cursor_id = cursor["id"]
            if order == SortOrder.ASC:
                conditions.append(
                    or_(
                        sort_expr > cursor_sort,
                        and_(sort_expr == cursor_sort, t.c.srn > cursor_id),
                    )
                )
            else:
                conditions.append(
                    or_(
                        sort_expr < cursor_sort,
                        and_(sort_expr == cursor_sort, t.c.srn < cursor_id),
                    )
                )

        where_clause = and_(*conditions) if conditions else text("TRUE")

        stmt = (
            select(t.c.srn, t.c.published_at, t.c.metadata)
            .where(where_clause)
            .order_by(*order_clauses)
            .limit(limit)
        )

        result = await self.session.execute(stmt)
        return [
            RecordSummary(
                srn=RecordSRN.parse(row["srn"]),
                published_at=row["published_at"],
                metadata=row["metadata"],
            )
            for row in result.mappings()
        ]

    async def get_feature_catalog(self) -> list[FeatureCatalogEntry]:
        """List all feature tables with column schemas and record counts."""
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.pg_table,
            feature_tables_table.c.feature_schema,
        )
        result = await self.session.execute(stmt)
        catalog_rows = result.mappings().all()

        if not catalog_rows:
            return []

        # Fetch all record counts in a single UNION ALL query (avoid N+1)
        count_parts = [
            select(
                literal(row["hook_name"]).label("hook_name"),
                func.count(func.distinct(text("record_srn"))).label("cnt"),
            ).select_from(text(f"features.{quoted_name(row['pg_table'], quote=True)}"))
            for row in catalog_rows
        ]
        counts_result = await self.session.execute(union_all(*count_parts))
        counts_by_hook = {r["hook_name"]: r["cnt"] for r in counts_result.mappings()}

        entries: list[FeatureCatalogEntry] = []
        for row in catalog_rows:
            schema_data = row["feature_schema"]
            columns_raw = schema_data.get("columns", []) if isinstance(schema_data, dict) else []
            columns = [
                ColumnInfo(
                    name=col["name"],
                    type=col.get("json_type", "string"),
                    required=col.get("required", False),
                )
                for col in columns_raw
            ]

            entries.append(
                FeatureCatalogEntry(
                    hook_name=row["hook_name"],
                    columns=columns,
                    record_count=counts_by_hook.get(row["hook_name"], 0),
                )
            )

        return entries

    async def get_feature_table_schema(self, hook_name: str) -> FeatureCatalogEntry | None:
        """Look up a single feature table's schema by hook name."""
        stmt = select(
            feature_tables_table.c.hook_name,
            feature_tables_table.c.feature_schema,
        ).where(feature_tables_table.c.hook_name == hook_name)
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None

        schema_data = row["feature_schema"]
        columns_raw = schema_data.get("columns", []) if isinstance(schema_data, dict) else []
        columns = [
            ColumnInfo(
                name=col["name"],
                type=col.get("json_type", "string"),
                required=col.get("required", False),
            )
            for col in columns_raw
        ]

        return FeatureCatalogEntry(
            hook_name=row["hook_name"],
            columns=columns,
            record_count=0,
        )

    async def search_features(
        self,
        hook_name: str,
        filters: list[Filter],
        record_srn: RecordSRN | None,
        sort: str,
        order: SortOrder,
        cursor: dict[str, Any] | None,
        limit: int,
    ) -> list[FeatureRow]:
        """Build and execute a dynamic SQL query for feature row search."""
        # Look up pg_table and feature_schema from catalog
        pg_table_stmt = select(
            feature_tables_table.c.pg_table,
            feature_tables_table.c.feature_schema,
        ).where(feature_tables_table.c.hook_name == hook_name)
        pg_result = await self.session.execute(pg_table_stmt)
        pg_row = pg_result.mappings().first()
        if pg_row is None:
            return []
        pg_table: str = pg_row["pg_table"]
        feature_schema: dict = pg_row["feature_schema"]

        # Build Table with full column list from schema using local MetaData
        from osa.domain.shared.model.hook import ColumnDef
        from osa.infrastructure.persistence.column_mapper import map_column

        schema_columns = (
            feature_schema.get("columns", []) if isinstance(feature_schema, dict) else []
        )
        data_columns = [
            map_column(
                ColumnDef(
                    name=col["name"],
                    json_type=col.get("json_type", "string"),
                    format=col.get("format"),
                    required=col.get("required", False),
                )
            )
            for col in schema_columns
        ]

        local_meta = MetaData()
        ft = Table(
            pg_table,
            local_meta,
            Column("id", Integer, primary_key=True),
            Column("record_srn", String),
            Column("created_at", String),
            *data_columns,
            schema="features",
        )

        conditions: list[Any] = []

        # Record SRN filter
        if record_srn is not None:
            conditions.append(ft.c.record_srn == str(record_srn))

        # Column filters — all columns are known from schema
        for f in filters:
            col = ft.c[f.field]
            if f.operator == FilterOperator.EQ:
                conditions.append(col == f.value)
            elif f.operator == FilterOperator.CONTAINS:
                conditions.append(
                    cast(col, String).ilike(f"%{_escape_like(str(f.value))}%", escape="\\")
                )
            elif f.operator == FilterOperator.GTE:
                conditions.append(col >= f.value)
            elif f.operator == FilterOperator.LTE:
                conditions.append(col <= f.value)

        # Sort expression
        if sort == "id":
            sort_expr = ft.c.id
        else:
            sort_expr = ft.c[sort]

        if order == SortOrder.ASC:
            order_clauses = [sort_expr.asc(), ft.c.id.asc()]
        else:
            order_clauses = [sort_expr.desc(), ft.c.id.desc()]

        # Keyset cursor
        if cursor is not None:
            cursor_sort = cursor["s"]
            cursor_id = cursor["id"]
            if order == SortOrder.ASC:
                conditions.append(
                    or_(
                        sort_expr > cursor_sort,
                        and_(sort_expr == cursor_sort, ft.c.id > cursor_id),
                    )
                )
            else:
                conditions.append(
                    or_(
                        sort_expr < cursor_sort,
                        and_(sort_expr == cursor_sort, ft.c.id < cursor_id),
                    )
                )

        where_clause = and_(*conditions) if conditions else text("TRUE")

        auto_cols = {"id", "created_at"}
        stmt = (
            select(
                ft.c.id,
                ft.c.record_srn,
                *[
                    c
                    for c in ft.columns
                    if c.key not in auto_cols and c.key not in ("id", "record_srn")
                ],
            )
            .where(where_clause)
            .order_by(*order_clauses)
            .limit(limit)
        )

        result = await self.session.execute(stmt)
        feature_rows: list[FeatureRow] = []
        for row in result.mappings():
            row_dict = dict(row)
            row_id = row_dict.pop("id")
            rsrn = RecordSRN.parse(row_dict.pop("record_srn"))
            row_dict.pop("created_at", None)
            feature_rows.append(FeatureRow(row_id=row_id, record_srn=rsrn, data=row_dict))

        return feature_rows

    @staticmethod
    def _record_filter_clause(f: Filter, field_type: FieldType | None = None) -> Any:
        """Build a SQL clause for a single record metadata filter."""
        t = records_table
        if f.operator == FilterOperator.EQ:
            # Use JSONB @> containment (GIN-indexed)
            return t.c.metadata.op("@>")(cast(func.json_build_object(f.field, f.value), JSONB))
        elif f.operator == FilterOperator.CONTAINS:
            return t.c.metadata[f.field].astext.ilike(
                f"%{_escape_like(str(f.value))}%", escape="\\"
            )
        elif f.operator in (FilterOperator.GTE, FilterOperator.LTE):
            # Use typed casts: numeric for NUMBER, date for DATE, string fallback
            if field_type == FieldType.NUMBER:
                col_expr = cast(t.c.metadata[f.field].astext, Float)
                val = float(f.value)
            elif field_type == FieldType.DATE:
                col_expr = cast(t.c.metadata[f.field].astext, Date)
                val = str(f.value)
            else:
                col_expr = cast(t.c.metadata[f.field].astext, String)
                val = str(f.value)
            if f.operator == FilterOperator.GTE:
                return col_expr >= val
            else:
                return col_expr <= val
        else:
            raise ValueError(f"Unknown operator: {f.operator}")  # pragma: no cover
