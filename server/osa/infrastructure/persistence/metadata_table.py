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
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.tables import records_table

METADATA_SCHEMA = "metadata"

AUTO_COLUMN_NAMES = frozenset({"id", "record_srn", "created_at"})

_SLUG_RE = re.compile(r"^[a-z][a-z0-9_]{0,50}$")


class MetadataSchema(ValueObject):
    """Typed representation of the ``metadata_tables.metadata_schema`` JSON column."""

    columns: list[ColumnDef] = []


def schema_slug(title: str) -> str:
    """Derive a pg-safe slug from a Schema title.

    Lowercases, replaces runs of non-alphanumerics with a single underscore,
    strips leading/trailing underscores, then validates against ``^[a-z][a-z0-9_]{0,50}$``.
    Raises ``ValueError`` if the derived slug is empty or cannot be validated.
    """
    normalised = re.sub(r"[^a-z0-9]+", "_", title.strip().lower()).strip("_")
    if not normalised or not _SLUG_RE.match(normalised):
        raise ValueError(
            f"Cannot derive a valid metadata table slug from title {title!r}. "
            f"Expected a string that maps to ^[a-z][a-z0-9_]{{0,50}}$."
        )
    return normalised


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
        schema=METADATA_SCHEMA,
    )


def data_columns(table: sa.Table) -> list[sa.Column]:
    """Return only the user-defined data columns, excluding auto columns."""
    return [c for c in table.columns if c.key not in AUTO_COLUMN_NAMES]
