"""Shared helpers for building dynamic feature Table objects from catalog schema."""

from __future__ import annotations

import sqlalchemy as sa

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.value import ValueObject
from osa.infrastructure.persistence.column_mapper import map_column
from osa.infrastructure.persistence.tables import records_table

FEATURES_SCHEMA = "features"

AUTO_COLUMN_NAMES = frozenset({"id", "record_srn", "created_at"})


class FeatureSchema(ValueObject):
    """Typed representation of the ``feature_tables.feature_schema`` JSON column.

    Serialised with :meth:`model_dump`, deserialised with :meth:`model_validate`.
    """

    columns: list[ColumnDef] = []


def build_feature_table(pg_table: str, schema: FeatureSchema) -> sa.Table:
    """Build a SQLAlchemy ``Table`` for a dynamic feature table.

    Returns a ``Table`` with auto columns (``id``, ``record_srn``, ``created_at``)
    plus data columns derived from *schema*, in the ``features`` PG schema.

    ``record_srn`` carries an ``ON DELETE CASCADE`` FK to ``records.srn`` — the
    FK target is the ``Column`` object itself (not a string reference), so
    SQLAlchemy resolves it without requiring ``records`` to live in the same
    disposable ``MetaData`` as the dynamic table.
    """
    data_columns = [map_column(col_def) for col_def in schema.columns]

    metadata = sa.MetaData()
    return sa.Table(
        pg_table,
        metadata,
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "record_srn",
            sa.Text,
            sa.ForeignKey(records_table.c.srn, ondelete="CASCADE"),
            nullable=False,
            index=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        *data_columns,
        schema=FEATURES_SCHEMA,
    )


def data_columns(table: sa.Table) -> list[sa.Column]:
    """Return only the user-defined data columns, excluding auto columns."""
    return [c for c in table.columns if c.key not in AUTO_COLUMN_NAMES]
