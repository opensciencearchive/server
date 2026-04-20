"""PostgreSQL implementation of MetadataStore.

Schema-keyed DDL lifecycle: one metadata table per (schema_identity, major
version) pair. The catalog row in ``public.metadata_tables`` is updated in
lock-step with ALTER ADD COLUMN operations so reads can reconstruct the
dynamic table shape without reflection.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal, Sequence

import sqlalchemy as sa
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from osa.domain.metadata.port.metadata_store import MetadataStore
from osa.domain.semantics.model.value import FieldDefinition, FieldType
from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.srn import RecordSRN, SchemaSRN
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.metadata_table import (
    METADATA_SCHEMA,
    MetadataSchema,
    build_metadata_table,
    schema_slug,
)
from osa.infrastructure.persistence.tables import metadata_tables_table


_JsonType = Literal["string", "number", "integer", "boolean", "array", "object"]


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


def _identity_of(schema_srn: SchemaSRN) -> str:
    """Return the version-stripped schema SRN (the schema identity)."""
    rendered = str(schema_srn)
    return rendered.split("@", 1)[0]


class PostgresMetadataStore(MetadataStore):
    """DDL + DML for per-schema typed metadata tables."""

    def __init__(self, engine: AsyncEngine, session: AsyncSession) -> None:
        self._engine = engine
        self._session = session

    async def ensure_table(
        self,
        schema_srn: SchemaSRN,
        schema_title: str,
        fields: list[FieldDefinition],
    ) -> None:
        identity = _identity_of(schema_srn)
        major = int(schema_srn.version.root.split(".")[0])
        slug = schema_slug(schema_title)
        pg_table = f"{slug}_v{major}"

        columns = [_field_to_column(f) for f in fields]
        metadata_schema = MetadataSchema(columns=columns)

        async with self._engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{METADATA_SCHEMA}"'))

            existing = (
                (
                    await conn.execute(
                        select(metadata_tables_table).where(
                            metadata_tables_table.c.schema_identity == identity,
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
                        schema_identity=identity,
                        schema_slug=slug,
                        schema_major=major,
                        schema_versions=[str(schema_srn)],
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
            if not new_columns:
                if str(schema_srn) not in stored_versions:
                    stored_versions.append(str(schema_srn))
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
            if str(schema_srn) not in stored_versions:
                stored_versions.append(str(schema_srn))
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
        schema_srn: SchemaSRN,
        record_srn: RecordSRN,
        values: dict[str, Any],
    ) -> None:
        identity = _identity_of(schema_srn)
        major = int(schema_srn.version.root.split(".")[0])

        catalog_row = (
            (
                await self._session.execute(
                    select(metadata_tables_table).where(
                        metadata_tables_table.c.schema_identity == identity,
                        metadata_tables_table.c.schema_major == major,
                    )
                )
            )
            .mappings()
            .first()
        )

        if catalog_row is None:
            raise ValidationError(
                f"No metadata table registered for schema {schema_srn} "
                f"(identity={identity}, major={major}). "
                "Ensure the convention has been registered first.",
                field="schema_srn",
            )

        schema = MetadataSchema.model_validate(catalog_row["metadata_schema"])
        pg_table = catalog_row["pg_table"]
        table = build_metadata_table(pg_table, schema)

        known = {c.name for c in schema.columns}
        payload = {k: v for k, v in values.items() if k in known}
        payload["record_srn"] = str(record_srn)

        stmt = insert(table).values(**payload)
        update_cols = {c: stmt.excluded[c] for c in payload.keys() if c != "record_srn"}
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
    """SQL string to ALTER TABLE ADD COLUMN for a single column definition."""
    sql_type = _column_type_sql(map_column(col_def).type)
    null_sql = "" if not col_def.required else " NOT NULL"
    return (
        f'ALTER TABLE "{METADATA_SCHEMA}"."{pg_table}" '
        f'ADD COLUMN IF NOT EXISTS "{col_def.name}" {sql_type}{null_sql}'
    )


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
