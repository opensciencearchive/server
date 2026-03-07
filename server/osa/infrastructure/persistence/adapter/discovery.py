"""Infrastructure adapters for the discovery domain — read-only SQL queries."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import (
    Date,
    Float,
    String,
    and_,
    cast,
    func,
    literal,
    or_,
    select,
    true,
    union_all,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession

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
from osa.infrastructure.persistence.feature_table import (
    FeatureSchema,
    build_feature_table,
    data_columns,
)
from osa.infrastructure.persistence.keyset import KeysetPage, SortKey
from osa.infrastructure.persistence.tables import (
    feature_tables_table,
    records_table,
    schemas_table,
)

logger = logging.getLogger(__name__)


def _escape_like(value: str) -> str:
    """Escape LIKE metacharacters so user input is matched literally."""
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _to_column_info(schema: FeatureSchema) -> list[ColumnInfo]:
    """Map typed FeatureSchema columns to API-facing ColumnInfo list."""
    return [ColumnInfo(name=c.name, type=c.json_type, required=c.required) for c in schema.columns]


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

        # Keyset pagination with correct NULL handling
        is_desc = order == SortOrder.DESC
        page = KeysetPage(
            [
                SortKey(sort_expr, descending=is_desc, nulls_last=True),
                SortKey(t.c.srn, descending=is_desc),
            ]
        )
        order_clauses = page.order_by()

        if cursor is not None:
            conditions.append(page.after((cursor["s"], cursor["id"])))

        where_clause = and_(*conditions) if conditions else true()

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

        # Parse schemas at the boundary
        parsed = [
            (row["hook_name"], FeatureSchema.model_validate(row["feature_schema"]), row["pg_table"])
            for row in catalog_rows
        ]

        # Fetch all record counts in a single UNION ALL query (avoid N+1)
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
                columns=_to_column_info(schema),
                record_count=counts_by_hook.get(hook_name, 0),
            )
            for hook_name, schema, _pg_table in parsed
        ]

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

        schema = FeatureSchema.model_validate(row["feature_schema"])
        return FeatureCatalogEntry(
            hook_name=row["hook_name"],
            columns=_to_column_info(schema),
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
        schema = FeatureSchema.model_validate(pg_row["feature_schema"])

        ft = build_feature_table(pg_table, schema)

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

        # Keyset pagination with correct NULL handling
        is_desc = order == SortOrder.DESC
        page = KeysetPage(
            [
                SortKey(sort_expr, descending=is_desc, nulls_last=True),
                SortKey(ft.c.id, descending=is_desc),
            ]
        )
        order_clauses = page.order_by()

        if cursor is not None:
            conditions.append(page.after((cursor["s"], cursor["id"])))

        where_clause = and_(*conditions) if conditions else true()

        stmt = (
            select(ft.c.id, ft.c.record_srn, *data_columns(ft))
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
