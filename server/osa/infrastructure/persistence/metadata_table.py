"""Shared helpers for building dynamic metadata Table objects.

Mirrors :mod:`osa.infrastructure.persistence.feature_table` — metadata tables
are schema-keyed typed stores living in the ``metadata`` PG schema, with a
catalog row in ``public.metadata_tables`` per (schema_identity, major) pair.
"""

from __future__ import annotations

import re

import sqlalchemy as sa

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.value import ValueObject
from osa.infrastructure.persistence.api_naming import metadata_pg_schema
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.tables import records_table

# Back-compat re-export for callers that import the constant directly.
# Prefer ``metadata_pg_schema()`` in new code.
METADATA_SCHEMA = metadata_pg_schema()

AUTO_COLUMN_NAMES = frozenset({"id", "record_srn", "created_at"})

# PG identifier limit under default ``NAMEDATALEN`` (64). Identifiers over
# this are silently truncated by PG, which would cause catalog/table name
# drift — surface the limit as a hard check instead.
PG_IDENT_MAX_LEN = 63

# Upper bound for a derived slug — matches :class:`SchemaIdentifier` (3-64).
# The final table name is ``f"{slug}_v{major}"``; that total length is
# checked separately by :func:`check_pg_table_name` at the boundary where
# ``major`` is known.
_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


class MetadataSchema(ValueObject):
    """Typed representation of the ``metadata_tables.metadata_schema`` JSON column."""

    columns: list[ColumnDef] = []


def schema_slug(title: str) -> str:
    """Derive a pg-safe slug from a Schema title.

    Lowercases, replaces runs of non-alphanumerics with a single underscore,
    strips leading/trailing underscores, then validates against
    ``^[a-z][a-z0-9_]{2,63}$`` (3-64 chars, matching
    :class:`SchemaIdentifier`). Raises ``ValueError`` if the derived slug is
    empty or cannot be validated.

    Callers that combine the slug with a suffix (e.g. ``_v{major}``) must
    separately check the combined length against :data:`PG_IDENT_MAX_LEN`.
    """
    normalised = re.sub(r"[^a-z0-9]+", "_", title.strip().lower()).strip("_")
    if not normalised or not _SLUG_RE.match(normalised):
        raise ValueError(
            f"Cannot derive a valid metadata table slug from title {title!r}. "
            "Expected a string that maps to ^[a-z][a-z0-9_]{2,63}$."
        )
    return normalised


def check_pg_table_name(pg_table: str) -> None:
    """Raise ``ValueError`` if *pg_table* exceeds the PG identifier limit.

    Without this, PG silently truncates long identifiers at 63 chars, which
    would desynchronise the catalog (``metadata_tables.pg_table``) from the
    actual table name.
    """
    if len(pg_table) > PG_IDENT_MAX_LEN:
        raise ValueError(
            f"Derived PG table name {pg_table!r} is {len(pg_table)} chars, "
            f"exceeds PG's {PG_IDENT_MAX_LEN}-char identifier limit. "
            "Use a shorter schema id."
        )


def build_metadata_table(pg_table: str, schema: MetadataSchema) -> sa.Table:
    """Build a SQLAlchemy ``Table`` for a dynamic metadata table.

    Adds auto columns (``id``, ``record_srn``, ``created_at``) plus data columns
    derived from *schema*. ``record_srn`` is ``UNIQUE`` (exactly one metadata
    row per record) and carries an ``ON DELETE CASCADE`` FK to ``records.srn``.
    The FK target is the ``Column`` object itself, so SQLAlchemy resolves it
    without requiring ``records`` to live in the same disposable ``MetaData``
    as the dynamic table.
    """
    data_columns = [map_column(col_def) for col_def in schema.columns]

    metadata_obj = sa.MetaData()
    return sa.Table(
        pg_table,
        metadata_obj,
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "record_srn",
            sa.Text,
            sa.ForeignKey(records_table.c.srn, ondelete="CASCADE"),
            nullable=False,
            unique=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        *data_columns,
        schema=metadata_pg_schema(),
    )


def data_columns(table: sa.Table) -> list[sa.Column]:
    """Return only the user-defined data columns, excluding auto columns."""
    return [c for c in table.columns if c.key not in AUTO_COLUMN_NAMES]
