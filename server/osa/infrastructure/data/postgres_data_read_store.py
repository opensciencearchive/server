"""Postgres adapter for the ``DataReadStore`` port.

The streaming primitive (:meth:`stream_rows`) builds the records / feature
SELECT, then iterates it through ``AsyncSession.stream()`` — a server-side
cursor (research §2). Rows are yielded one at a time as flattened column→value
mappings, so memory stays bounded regardless of result size. The ``async with``
around the streaming result closes the cursor on client disconnect, returning
the connection to the pool.

The records filter compilation reuses the proven approach from the discovery
adapter, ported onto the ``data`` domain's own filter model so the adapter has
no dependency on ``discovery`` once that domain is deleted (research §10).
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Mapping
from datetime import date, datetime
from typing import Any

import sqlalchemy as sa
from sqlalchemy import (
    RowMapping,
    String,
    and_,
    cast,
    false,
    func,
    not_,
    or_,
    select,
)
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.data.model.catalog import (
    CatalogEntry,
    NodeCatalog,
    TableResourceSummary,
)
from osa.domain.data.model.filter import (
    And,
    FeatureFieldRef,
    FilterExpr,
    FilterOperator,
    MetadataFieldRef,
    Not,
    Or,
    Predicate,
)
from osa.domain.data.model.manifest import (
    IMPLICIT_FEATURE_COLUMN_SPECS,
    IMPLICIT_RECORD_COLUMN_SPECS,
    ColumnSpec,
    FieldSpec,
    SchemaManifest,
    TableResource,
)
from osa.domain.data.model.query_plan import QueryPlan, SortDirection, TableKind
from osa.domain.data.model.record_summary import RecordSummary
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.error import NotFoundError, ValidationError
from osa.domain.shared.model.ids import RecordId
from osa.domain.shared.model.srn import Domain, RecordSRN, SchemaId
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
    conventions_table,
    feature_tables_table,
    metadata_tables_table,
    records_table,
    schemas_table,
)

logger = logging.getLogger(__name__)

# A feature column's JSON-primitive type → the manifest's semantic FieldType.
_JSON_TYPE_TO_FIELD_TYPE: dict[str, FieldType] = {
    "string": FieldType.TEXT,
    "number": FieldType.NUMBER,
    "integer": FieldType.NUMBER,
    "boolean": FieldType.BOOLEAN,
    "array": FieldType.TEXT,
    "object": FieldType.TEXT,
}

# All URL-exposed format suffixes (mirrors model.format.FORMATS suffixes).
_ALL_FORMATS = ["", "csv", "csv.gz"]


def _escape_like(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _invalid_cursor(exc: Exception) -> ValidationError:
    """Map a cursor decode/coerce ``ValueError`` to a 400, not a 500.

    Decoding a corrupt cursor (bad base64, missing ``s``/``id`` keys) or coercing
    a non-conforming value (e.g. a non-integer feature id) raises ``ValueError``,
    which is not an ``OSAError`` and would otherwise surface as a generic 500.
    The cursor is opaque client-supplied input, so a malformed one is a 400.
    """
    return ValidationError(
        f"Malformed pagination cursor: {exc}", field="cursor", code="invalid_cursor"
    )


class PostgresDataReadStore:
    def __init__(self, session: AsyncSession, node_domain: Domain) -> None:
        self.session = session
        # Only the node's DNS domain is needed (to render SRNs in the catalog /
        # manifest) — not the whole Config.
        self.node_domain = node_domain

    # ------------------------------------------------------------------ #
    # Streaming primitive
    # ------------------------------------------------------------------ #

    async def stream_rows(self, plan: QueryPlan) -> AsyncIterator[Mapping[str, Any]]:
        if plan.table_kind == TableKind.RECORDS:
            async for row in self._stream_records(plan):
                yield row
        else:  # TableKind.FEATURE
            async for row in self._stream_features(plan):
                yield row

    async def _stream_records(self, plan: QueryPlan) -> AsyncIterator[Mapping[str, Any]]:
        catalog = await self._metadata_catalog_for(plan.schema_id)
        if catalog is None:
            raise NotFoundError(
                f"Schema not found: {plan.schema_id.render()}. See /api/v1/data for the catalog."
            )
        metadata_schema = MetadataSchema.model_validate(catalog["metadata_schema"])
        metadata_table = build_metadata_table(catalog["pg_table"], metadata_schema)

        t = records_table
        conditions: list[Any] = [
            t.c.schema_id == plan.schema_id.id.root,
            t.c.schema_version == plan.schema_id.version.root,
        ]
        if plan.filter is not None:
            conditions.append(self._compile_filter(plan.filter, metadata_t=metadata_table))

        order_keys, cursor_after = self._records_sort(plan, metadata_table)
        if cursor_after is not None:
            conditions.append(cursor_after)

        select_cols = [
            t.c.srn,
            t.c.schema_id,
            t.c.schema_version,
            t.c.published_at,
        ] + [metadata_table.c[c.name].label(c.name) for c in metadata_schema.columns]
        stmt = (
            select(*select_cols)
            .select_from(t.join(metadata_table, metadata_table.c.record_srn == t.c.srn))
            .where(and_(*conditions))
            .order_by(*order_keys)
        )

        col_names = [c.name for c in metadata_schema.columns]
        # ``stream()`` opens a server-side cursor. The try/finally closes it on
        # client disconnect (the generator is thrown a CancelledError), returning
        # the connection to the pool (research §2).
        result = await self.session.stream(stmt)
        try:
            async for row in result.mappings():
                yield self._records_row_to_mapping(row, col_names)
        finally:
            await result.close()

    def _records_row_to_mapping(self, row: RowMapping, col_names: list[str]) -> dict[str, Any]:
        srn = RecordSRN.parse(row["srn"])
        summary = RecordSummary(
            id=RecordId(srn.id.root),
            srn=srn,
            schema_id=SchemaId.parse(f"{row['schema_id']}@{row['schema_version']}"),
            version=int(srn.version.root),
            metadata={name: row[name] for name in col_names if name in row},
            created_at=row["published_at"],
        )
        return summary.flatten()

    def _records_sort(self, plan: QueryPlan, metadata_table: Any) -> tuple[list[Any], Any | None]:
        t = records_table
        # First sort key drives the cursor value; ``id`` (srn) is the tiebreaker.
        primary = plan.sort[0]
        is_desc = primary.direction == SortDirection.DESC
        if primary.column in ("created_at", "published_at"):
            sort_expr = t.c.published_at
        elif primary.column == "id":
            sort_expr = t.c.srn
        elif primary.column in metadata_table.c:
            sort_expr = metadata_table.c[primary.column]
        else:
            raise ValidationError(
                f"Unknown sort column '{primary.column}'.", field="sort", code="unknown_sort_field"
            )
        page = KeysetPage(
            [
                SortKey(sort_expr, descending=is_desc, nulls_last=True),
                SortKey(t.c.srn, descending=is_desc),
            ]
        )
        cursor_after = None
        if plan.pagination.cursor is not None:
            from osa.domain.data.model.query_plan import decode_cursor

            try:
                decoded = decode_cursor(str(plan.pagination.cursor))
                sort_value = self._coerce_cursor_value(decoded["s"], primary.column)
                cursor_after = page.after((sort_value, decoded["id"]))
            except ValueError as exc:
                raise _invalid_cursor(exc) from exc
        return page.order_by(), cursor_after

    @staticmethod
    def _coerce_cursor_value(value: Any, column: str) -> Any:
        if column in ("created_at", "published_at") and isinstance(value, str):
            return datetime.fromisoformat(value)
        return value

    # ------------------------------------------------------------------ #
    # Feature-table streaming (US5)
    # ------------------------------------------------------------------ #

    async def _stream_features(self, plan: QueryPlan) -> AsyncIterator[Mapping[str, Any]]:
        if plan.feature_name is None:  # guarded by QueryPlan, narrowed for the type checker
            raise ValidationError("feature_name is required for a FEATURE plan", field="feature")
        ft, fschema = await self._resolve_feature_table(plan.schema_id, plan.feature_name)

        conditions: list[Any] = []
        if plan.filter is not None:
            conditions.append(
                self._compile_feature_filter(plan.filter, ft=ft, feature_name=plan.feature_name)
            )

        order_keys, cursor_after = self._features_sort(plan, ft, fschema)
        if cursor_after is not None:
            conditions.append(cursor_after)

        # Implicit columns (id, record_srn, created_at) precede the hook's
        # declared data columns — this is the CSV header order.
        select_cols = [ft.c.id, ft.c.record_srn, ft.c.created_at, *data_columns(ft)]
        stmt = select(*select_cols).select_from(ft)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        stmt = stmt.order_by(*order_keys)

        result = await self.session.stream(stmt)
        try:
            async for row in result.mappings():
                yield dict(row)
        finally:
            await result.close()

    async def _resolve_feature_table(
        self, schema_id: SchemaId, feature_name: str
    ) -> tuple[sa.Table, FeatureSchema]:
        """Resolve a feature table that belongs to ``schema_id``.

        A feature (hook) belongs to a schema via the convention that registers
        it. Streaming a feature not registered on the schema is a 404, not a
        leak of another schema's table.
        """
        for hook_name, fschema in await self._schema_feature_tables(schema_id):
            if hook_name == feature_name:
                return build_feature_table(hook_name, fschema), fschema
        raise NotFoundError(
            f"No feature table '{feature_name}' on schema {schema_id.render()}. "
            f"See /api/v1/data/{schema_id.render()} for its table resources.",
            code="feature_not_found",
        )

    async def _schema_hook_names(self, schema_id: SchemaId) -> set[str]:
        """Hook names registered on the schema's conventions (the schema→feature link)."""
        stmt = select(conventions_table.c.hooks).where(
            conventions_table.c.schema_id == schema_id.id.root,
            conventions_table.c.schema_version == schema_id.version.root,
        )
        result = await self.session.execute(stmt)
        names: set[str] = set()
        for (hooks,) in result.all():
            for hook in hooks or []:
                names.add(hook["name"])
        return names

    async def _schema_feature_tables(self, schema_id: SchemaId) -> list[tuple[str, FeatureSchema]]:
        """(hook_name, FeatureSchema) for every materialized feature table on the schema."""
        hook_names = await self._schema_hook_names(schema_id)
        if not hook_names:
            return []
        stmt = select(
            feature_tables_table.c.hook_name, feature_tables_table.c.feature_schema
        ).where(feature_tables_table.c.hook_name.in_(hook_names))
        result = await self.session.execute(stmt)
        return [
            (row["hook_name"], FeatureSchema.model_validate(row["feature_schema"]))
            for row in result.mappings()
        ]

    async def _feature_count(self, ft: sa.Table) -> int:
        return int((await self.session.execute(select(func.count()).select_from(ft))).scalar_one())

    def _features_sort(
        self, plan: QueryPlan, ft: sa.Table, fschema: FeatureSchema
    ) -> tuple[list[Any], Any | None]:
        primary = plan.sort[0]
        is_desc = primary.direction == SortDirection.DESC
        if primary.column == "id":
            sort_expr = ft.c.id
        elif primary.column in ft.c:
            sort_expr = ft.c[primary.column]
        else:
            raise ValidationError(
                f"Unknown sort column '{primary.column}'.",
                field="sort",
                code="unknown_sort_field",
            )
        page = KeysetPage(
            [
                SortKey(sort_expr, descending=is_desc, nulls_last=True),
                SortKey(ft.c.id, descending=is_desc),
            ]
        )
        cursor_after = None
        if plan.pagination.cursor is not None:
            from osa.domain.data.model.query_plan import decode_cursor

            try:
                decoded = decode_cursor(str(plan.pagination.cursor))
                sort_value = self._coerce_feature_cursor_value(
                    decoded["s"], primary.column, fschema
                )
                cursor_after = page.after((sort_value, int(decoded["id"])))
            except ValueError as exc:
                raise _invalid_cursor(exc) from exc
        return page.order_by(), cursor_after

    @staticmethod
    def _coerce_feature_cursor_value(value: Any, column: str, fschema: FeatureSchema) -> Any:
        if column == "id":
            return None if value is None else int(value)
        col_def = next((c for c in fschema.columns if c.name == column), None)
        if col_def is not None and col_def.json_type == "string" and isinstance(value, str):
            if col_def.format == "date-time":
                return datetime.fromisoformat(value)
            if col_def.format == "date":
                return date.fromisoformat(value)
        return value

    def _compile_feature_filter(self, expr: FilterExpr, *, ft: sa.Table, feature_name: str) -> Any:
        if isinstance(expr, Predicate):
            return self._compile_feature_predicate(expr, ft=ft, feature_name=feature_name)
        if isinstance(expr, And):
            return and_(
                *[
                    self._compile_feature_filter(op, ft=ft, feature_name=feature_name)
                    for op in expr.operands
                ]
            )
        if isinstance(expr, Or):
            return or_(
                *[
                    self._compile_feature_filter(op, ft=ft, feature_name=feature_name)
                    for op in expr.operands
                ]
            )
        if isinstance(expr, Not):
            inner = self._compile_feature_filter(expr.operand, ft=ft, feature_name=feature_name)
            return not_(func.coalesce(inner, false()))
        raise ValidationError(f"Unsupported filter node: {type(expr).__name__}")

    def _compile_feature_predicate(
        self, predicate: Predicate, *, ft: sa.Table, feature_name: str
    ) -> Any:
        if isinstance(predicate.field, FeatureFieldRef):
            if predicate.field.hook != feature_name:
                raise ValidationError(
                    f"Cross-feature predicates are not supported on the "
                    f"'{feature_name}' stream (referenced '{predicate.field.hook}').",
                    field=predicate.field.dotted(),
                    code="cross_feature_predicate_unsupported",
                )
            if predicate.field.column not in ft.c:
                raise ValidationError(
                    f"Unknown feature column '{predicate.field.column}'.",
                    field=predicate.field.dotted(),
                    code="unknown_feature_column",
                )
            return _apply_scalar_op(ft.c[predicate.field.column], predicate.op, predicate.value)
        if isinstance(predicate.field, MetadataFieldRef):
            raise ValidationError(
                "Metadata-field predicates are not supported on a feature stream.",
                field=predicate.field.dotted(),
                code="metadata_predicate_unsupported",
            )
        raise TypeError(f"Unexpected field ref type: {type(predicate.field).__name__}")

    # ------------------------------------------------------------------ #
    # Single record by ID
    # ------------------------------------------------------------------ #

    async def get_record_by_id(self, id: RecordId, version: int | None) -> RecordSummary | None:
        # The records PK is the SRN ``urn:osa:{domain}:rec:{id}@{version}``.
        # Match the id segment; resolve version (pin or latest published).
        pattern = f"urn:osa:%:rec:{_escape_like(str(id))}@%"
        t = records_table
        stmt = (
            select(t.c.srn, t.c.schema_id, t.c.schema_version, t.c.published_at, t.c.metadata)
            .where(t.c.srn.like(pattern, escape="\\"))
            .order_by(t.c.published_at.desc())
        )
        result = await self.session.execute(stmt)
        rows = result.mappings().all()
        if not rows:
            return None

        chosen = None
        for row in rows:
            srn = RecordSRN.parse(row["srn"])
            if srn.id.root != str(id):
                continue
            if version is None:
                chosen = (srn, row)
                break
            if int(srn.version.root) == version:
                chosen = (srn, row)
                break
        if chosen is None:
            return None
        srn, row = chosen
        return RecordSummary(
            id=RecordId(srn.id.root),
            srn=srn,
            schema_id=SchemaId.parse(f"{row['schema_id']}@{row['schema_version']}"),
            version=int(srn.version.root),
            metadata=row["metadata"] or {},
            created_at=row["published_at"],
        )

    # ------------------------------------------------------------------ #
    # Catalog & manifest
    # ------------------------------------------------------------------ #

    async def get_node_catalog(self) -> NodeCatalog:
        stmt = select(schemas_table.c.id, schemas_table.c.version)
        result = await self.session.execute(stmt)
        schema_rows = [(row["id"], row["version"]) for row in result.mappings()]
        entries: list[CatalogEntry] = []
        for short_id, version in schema_rows:
            schema_id = SchemaId.parse(f"{short_id}@{version}")
            resources = [TableResourceSummary(name="records", kind=TableKind.RECORDS)]
            for hook_name, _ in await self._schema_feature_tables(schema_id):
                resources.append(TableResourceSummary(name=hook_name, kind=TableKind.FEATURE))
            entries.append(
                CatalogEntry(
                    id=short_id,
                    version=version,
                    srn=schema_id.to_srn(self.node_domain).render(),
                    table_resources=resources,
                )
            )
        return NodeCatalog(node_domain=self.node_domain.root, schemas=entries)

    async def get_schema_manifest(self, schema_id: SchemaId) -> SchemaManifest | None:
        stmt = select(schemas_table.c.fields).where(
            schemas_table.c.id == schema_id.id.root,
            schemas_table.c.version == schema_id.version.root,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        if row is None:
            return None

        field_specs: list[FieldSpec] = []
        column_specs: list[ColumnSpec] = []
        for f in row["fields"]:
            ftype = FieldType(f["type"])
            field_specs.append(
                FieldSpec(
                    name=f["name"],
                    type=ftype,
                    ontology_id=f.get("ontology_id"),
                    ontology_version=f.get("ontology_version"),
                )
            )
            column_specs.append(ColumnSpec(name=f["name"], type=ftype))

        record_count = await self._records_count(schema_id)
        records_resource = TableResource(
            name="records",
            kind=TableKind.RECORDS,
            # Implicit columns (id, srn, schema_id, version, created_at) precede
            # the schema's declared metadata fields — this is the CSV header order.
            columns=[*IMPLICIT_RECORD_COLUMN_SPECS, *column_specs],
            row_count=record_count,
            formats=list(_ALL_FORMATS),
        )
        feature_resources = await self._feature_resources(schema_id)
        return SchemaManifest(
            id=schema_id.id.root,
            version=schema_id.version.root,
            srn=schema_id.to_srn(self.node_domain).render(),
            fields=field_specs,
            table_resources=[records_resource, *feature_resources],
        )

    async def _feature_resources(self, schema_id: SchemaId) -> list[TableResource]:
        """Build a TableResource for each feature table registered on the schema."""
        resources: list[TableResource] = []
        for hook_name, fschema in await self._schema_feature_tables(schema_id):
            ft = build_feature_table(hook_name, fschema)
            resources.append(
                TableResource(
                    name=hook_name,
                    kind=TableKind.FEATURE,
                    # Implicit columns (id, record_srn, created_at) precede the
                    # hook's declared data columns — this is the CSV header order.
                    columns=[*IMPLICIT_FEATURE_COLUMN_SPECS, *_feature_column_specs(fschema)],
                    row_count=await self._feature_count(ft),
                    formats=list(_ALL_FORMATS),
                )
            )
        return resources

    async def get_latest_schema_id(self, schema_short_id: str) -> SchemaId | None:
        stmt = select(schemas_table.c.version).where(schemas_table.c.id == schema_short_id)
        result = await self.session.execute(stmt)
        versions = [row[0] for row in result.all()]
        if not versions:
            return None
        # Pick the highest SemVer (string sort is wrong for e.g. 1.10.0 vs 1.9.0).
        latest = max(versions, key=lambda v: tuple(int(p) for p in v.split("-")[0].split(".")))
        return SchemaId.parse(f"{schema_short_id}@{latest}")

    async def _records_count(self, schema_id: SchemaId) -> int:
        t = records_table
        stmt = (
            select(func.count())
            .select_from(t)
            .where(
                t.c.schema_id == schema_id.id.root,
                t.c.schema_version == schema_id.version.root,
            )
        )
        return int((await self.session.execute(stmt)).scalar_one())

    # ------------------------------------------------------------------ #
    # Helpers (ported from the discovery adapter, records-only)
    # ------------------------------------------------------------------ #

    async def _metadata_catalog_for(self, schema_id: SchemaId) -> dict[str, Any] | None:
        stmt = select(metadata_tables_table).where(
            metadata_tables_table.c.schema_id == schema_id.id.root,
            metadata_tables_table.c.schema_major == schema_id.major,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return dict(row) if row is not None else None

    def _compile_filter(self, expr: FilterExpr, *, metadata_t: Any) -> Any:
        if isinstance(expr, Predicate):
            return self._compile_predicate(expr, metadata_t=metadata_t)
        if isinstance(expr, And):
            return and_(*[self._compile_filter(op, metadata_t=metadata_t) for op in expr.operands])
        if isinstance(expr, Or):
            return or_(*[self._compile_filter(op, metadata_t=metadata_t) for op in expr.operands])
        if isinstance(expr, Not):
            inner = self._compile_filter(expr.operand, metadata_t=metadata_t)
            # NULL → FALSE before negating so records with NULL metadata survive NOT.
            return not_(func.coalesce(inner, false()))
        raise ValidationError(f"Unsupported filter node: {type(expr).__name__}")

    def _compile_predicate(self, predicate: Predicate, *, metadata_t: Any) -> Any:
        if isinstance(predicate.field, MetadataFieldRef):
            if predicate.field.field not in metadata_t.c:
                raise ValidationError(
                    f"Unknown metadata field '{predicate.field.field}'. "
                    "Filterable fields are listed in the schema manifest.",
                    field=predicate.field.dotted(),
                    code="unknown_metadata_field",
                )
            col = metadata_t.c[predicate.field.field]
            return _apply_scalar_op(col, predicate.op, predicate.value)
        if isinstance(predicate.field, FeatureFieldRef):
            # Feature predicates in the records stream are a US5 extension.
            raise ValidationError(
                "Feature-field predicates are not yet supported on the records stream.",
                field=predicate.field.dotted(),
                code="feature_predicate_unsupported",
            )
        raise TypeError(f"Unexpected field ref type: {type(predicate.field).__name__}")


def _feature_column_specs(fschema: FeatureSchema) -> list[ColumnSpec]:
    """Map a feature table's declared columns to manifest ColumnSpecs."""
    return [
        ColumnSpec(name=c.name, type=_JSON_TYPE_TO_FIELD_TYPE[c.json_type]) for c in fschema.columns
    ]


def _apply_scalar_op(col: Any, op: FilterOperator, value: Any) -> Any:
    if op == FilterOperator.EQ:
        return col == value
    if op == FilterOperator.NEQ:
        return or_(col != value, col.is_(None))
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
                "Operator 'in' requires a list value.", field=col.key, code="invalid_value_for_op"
            )
        return col.in_(value)
    if op == FilterOperator.CONTAINS:
        return cast(col, String).ilike(f"%{_escape_like(str(value))}%", escape="\\")
    if op == FilterOperator.IS_NULL:
        return col.is_(None)
    raise ValidationError(
        f"Unsupported operator: {op}", field="filter", code="unsupported_operator"
    )
