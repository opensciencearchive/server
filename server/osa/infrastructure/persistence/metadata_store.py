"""PostgreSQL implementation of MetadataStore.

Schema-keyed DDL lifecycle: one metadata table per ``(schema_id, major)``
pair. The catalog row in ``public.metadata_tables`` is updated in lock-step
with ALTER ADD COLUMN operations so reads can reconstruct the dynamic table
shape without reflection.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import Any, Literal, Sequence

import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.metadata.port.metadata_store import MetadataStore
from osa.domain.semantics.model.value import FieldDefinition, FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import RecordSRN, SchemaId
from osa.infrastructure.persistence.api_naming import metadata_pg_schema
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.metadata_table import (
    MetadataSchema,
    build_metadata_table,
    check_pg_table_name,
    schema_slug,
)
from osa.infrastructure.persistence.tables import metadata_tables_table


_JsonType = Literal["string", "number", "integer", "boolean", "array", "object"]

# Defense-in-depth: validate any string interpolated into a raw DDL statement.
# ``ColumnDef.name`` is declared as ``PgIdentifier`` at the Pydantic layer but
# we re-check here because a) catalog rows round-trip through JSON and a bad
# actor with write access to metadata_tables could smuggle a malicious name
# through, and b) this function's contract should not rely on upstream
# validators that might be refactored away.
_PG_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def _safe_ident(name: str) -> str:
    if not _PG_IDENT_RE.match(name):
        raise ValidationError(f"Refusing to interpolate unsafe PG identifier {name!r} into DDL")
    return name


_JSON_TYPE_MAP: dict[FieldType, tuple[_JsonType | None, str | None]] = {
    FieldType.TEXT: ("string", None),
    FieldType.URL: ("string", None),
    FieldType.TERM: ("string", None),
    FieldType.DATE: ("string", "date"),
    FieldType.NUMBER: ("number", None),
    FieldType.BOOLEAN: ("boolean", None),
}


def _field_to_column(field: FieldDefinition) -> ColumnDef:
    """Translate a FieldDefinition into a ColumnDef for the metadata table."""
    json_type, fmt = _JSON_TYPE_MAP.get(field.type, (None, None))
    if json_type is None:
        raise ValidationError(
            f"Field {field.name!r} has unrepresentable type {field.type!r}. "
            "Add a column-mapper entry for this FieldType before using it in a schema.",
            field=field.name,
        )
    return ColumnDef(
        name=field.name,
        json_type=json_type,
        format=fmt,
        required=field.required,
    )


class PostgresMetadataStore(MetadataStore):
    """DDL + DML for per-schema typed metadata tables."""

    def __init__(self, engine: AsyncEngine, session: AsyncSession) -> None:
        self._engine = engine
        self._session = session

    async def ensure_table(
        self,
        schema_id: SchemaId,
        fields: list[FieldDefinition],
    ) -> None:
        id_str = schema_id.id.root
        major = schema_id.major
        try:
            slug = schema_slug(id_str)
        except ValueError as exc:
            raise ValidationError(str(exc), field="schema_id") from exc
        pg_table = f"{slug}_v{major}"
        try:
            check_pg_table_name(pg_table)
        except ValueError as exc:
            raise ValidationError(str(exc), field="schema_id") from exc

        columns = [_field_to_column(f) for f in fields]
        metadata_schema = MetadataSchema(columns=columns)

        async with self._engine.begin() as conn:
            # Note: the ``metadata`` PG schema is created by migration
            # ``076_add_metadata_schema_and_catalog`` and is a precondition
            # for this store. We don't run ``CREATE SCHEMA IF NOT EXISTS``
            # here because it races on ``pg_namespace`` under concurrency,
            # and the migration makes it unnecessary.

            # Serialise concurrent ensure_table() calls for the same
            # (schema_id, major) pair. Without this lock, two conventions
            # registering simultaneously both pass the "does it exist?"
            # check and race on CREATE TABLE, causing the loser to fail
            # with DuplicateTable. The advisory lock is released at
            # transaction commit.
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": f"{id_str}@v{major}"},
            )

            existing = (
                (
                    await conn.execute(
                        select(metadata_tables_table).where(
                            metadata_tables_table.c.schema_id == id_str,
                            metadata_tables_table.c.schema_major == major,
                        )
                    )
                )
                .mappings()
                .first()
            )

            if existing is None:
                table = build_metadata_table(pg_table, metadata_schema)
                await conn.run_sync(table.metadata.create_all, checkfirst=False)
                now = datetime.now(UTC)
                await conn.execute(
                    metadata_tables_table.insert().values(
                        schema_id=id_str,
                        schema_slug=slug,
                        schema_major=major,
                        schema_versions=[schema_id.render()],
                        pg_table=pg_table,
                        metadata_schema=metadata_schema.model_dump(),
                        created_at=now,
                        updated_at=now,
                    )
                )
                return

            # Table exists — possibly evolve.
            stored_schema = MetadataSchema.model_validate(existing["metadata_schema"])
            stored_versions: list[str] = list(existing["schema_versions"])
            pg_table = existing["pg_table"]

            _validate_additive(stored_schema.columns, columns)

            new_columns = [
                c for c in columns if c.name not in {s.name for s in stored_schema.columns}
            ]
            rendered = schema_id.render()
            if not new_columns:
                if rendered not in stored_versions:
                    stored_versions.append(rendered)
                    await conn.execute(
                        metadata_tables_table.update()
                        .where(metadata_tables_table.c.id == existing["id"])
                        .values(
                            schema_versions=stored_versions,
                            updated_at=datetime.now(UTC),
                        )
                    )
                return

            # Apply ALTER ADD COLUMN for each new column
            for col_def in new_columns:
                await conn.execute(text(_alter_add_column_stmt(pg_table, col_def)))

            merged_columns = stored_schema.columns + new_columns
            if rendered not in stored_versions:
                stored_versions.append(rendered)
            await conn.execute(
                metadata_tables_table.update()
                .where(metadata_tables_table.c.id == existing["id"])
                .values(
                    metadata_schema=MetadataSchema(columns=merged_columns).model_dump(),
                    schema_versions=stored_versions,
                    updated_at=datetime.now(UTC),
                )
            )

    async def insert(
        self,
        schema_id: SchemaId,
        record_srn: RecordSRN,
        values: dict[str, Any],
    ) -> None:
        await self.insert_many(schema_id, [(record_srn, values)])

    async def insert_many(
        self,
        schema_id: SchemaId,
        rows: list[tuple[RecordSRN, dict[str, Any]]],
    ) -> None:
        if not rows:
            return

        id_str = schema_id.id.root
        major = schema_id.major

        catalog_row = (
            (
                await self._session.execute(
                    select(metadata_tables_table).where(
                        metadata_tables_table.c.schema_id == id_str,
                        metadata_tables_table.c.schema_major == major,
                    )
                )
            )
            .mappings()
            .first()
        )

        if catalog_row is None:
            raise ValidationError(
                f"No metadata table registered for schema {schema_id.render()} "
                f"(id={id_str}, major={major}). "
                "Ensure the convention has been registered first.",
                field="schema_id",
            )

        schema = MetadataSchema.model_validate(catalog_row["metadata_schema"])
        pg_table = catalog_row["pg_table"]
        table = build_metadata_table(pg_table, schema)

        col_by_name = {c.name: c for c in schema.columns}
        known_names = set(col_by_name.keys())

        payloads: list[dict[str, Any]] = []
        for record_srn, values in rows:
            payload: dict[str, Any] = {}
            for k, v in values.items():
                col = col_by_name.get(k)
                if col is None:
                    continue
                payload[k] = _coerce_value(col, v)
            payload["record_srn"] = str(record_srn)
            payloads.append(payload)

        # Uniform column set across all rows — asyncpg multi-row insert requires it.
        # Fill missing columns with None so every payload has the same keys.
        all_keys: set[str] = {"record_srn"} | known_names
        for p in payloads:
            for k in all_keys:
                p.setdefault(k, None)

        stmt = insert(table).values(payloads)
        update_cols = {c: stmt.excluded[c] for c in all_keys if c != "record_srn"}
        if update_cols:
            stmt = stmt.on_conflict_do_update(
                index_elements=[table.c.record_srn],
                set_=update_cols,
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=[table.c.record_srn])
        await self._session.execute(stmt)
        await self._session.flush()


def _validate_additive(existing: Sequence[ColumnDef], incoming: Sequence[ColumnDef]) -> None:
    """Raise ValidationError if the incoming column set is not additive."""
    by_name = {c.name: c for c in existing}
    for col in incoming:
        if col.name not in by_name:
            if col.required:
                raise ValidationError(
                    f"Non-additive evolution: new field {col.name!r} is required. "
                    "New fields in minor/patch bumps must be optional.",
                    field=col.name,
                )
            continue
        prev = by_name[col.name]
        if prev.json_type != col.json_type or prev.format != col.format:
            raise ValidationError(
                f"Non-additive evolution: field {col.name!r} changed type "
                f"({prev.json_type}/{prev.format} → {col.json_type}/{col.format}).",
                field=col.name,
            )
        if prev.required is False and col.required is True:
            raise ValidationError(
                f"Non-additive evolution: field {col.name!r} tightened to required.",
                field=col.name,
            )
    incoming_names = {c.name for c in incoming}
    for prev_name in by_name.keys():
        if prev_name not in incoming_names:
            raise ValidationError(
                f"Non-additive evolution: field {prev_name!r} was removed.",
                field=prev_name,
            )


def _alter_add_column_stmt(pg_table: str, col_def: ColumnDef) -> str:
    """SQL string to ALTER TABLE ADD COLUMN for a single column definition.

    Both ``pg_table`` and ``col_def.name`` are interpolated into raw SQL, so
    they are strictly validated against the PG identifier regex first — any
    attempt to smuggle a ``"`` through would otherwise break the quoting and
    inject arbitrary DDL.
    """
    sql_type = _column_type_sql(map_column(col_def).type)
    null_sql = "" if not col_def.required else " NOT NULL"
    safe_table = _safe_ident(pg_table)
    safe_col = _safe_ident(col_def.name)
    return (
        f'ALTER TABLE "{metadata_pg_schema()}"."{safe_table}" '
        f'ADD COLUMN IF NOT EXISTS "{safe_col}" {sql_type}{null_sql}'
    )


def _coerce_value(col: ColumnDef, value: Any) -> Any:
    """Coerce a JSONB-read value to match its typed PG column.

    ``records.metadata`` is JSONB, so date/datetime fields come back as ISO
    strings. asyncpg won't auto-parse those for DATE / TIMESTAMP columns —
    we parse here based on the declared column format.
    """
    if value is None:
        return None
    if col.json_type == "string" and col.format == "date":
        return value if isinstance(value, date) else date.fromisoformat(value)
    if col.json_type == "string" and col.format == "date-time":
        return value if isinstance(value, datetime) else datetime.fromisoformat(value)
    return value


def _column_type_sql(sa_type: Any) -> str:
    if isinstance(sa_type, sa.Text):
        return "text"
    if isinstance(sa_type, sa.DateTime):
        return "timestamp with time zone" if sa_type.timezone else "timestamp"
    if isinstance(sa_type, sa.Date):
        return "date"
    if isinstance(sa_type, sa.Uuid):
        return "uuid"
    if isinstance(sa_type, sa.Float):
        return "double precision"
    if isinstance(sa_type, sa.BigInteger):
        return "bigint"
    if isinstance(sa_type, sa.Boolean):
        return "boolean"
    return "jsonb"
