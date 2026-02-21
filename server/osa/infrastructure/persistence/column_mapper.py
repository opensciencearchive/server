"""Map ColumnDef (JSON Schema types) to SQLAlchemy column types."""

from collections.abc import Callable
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

from osa.domain.shared.model.hook import ColumnDef

_TYPE_MAP: dict[tuple[str, str | None], Callable[[], Any]] = {
    ("string", None): sa.Text,
    ("string", "date-time"): lambda: sa.DateTime(timezone=True),
    ("string", "date"): sa.Date,
    ("string", "uuid"): sa.Uuid,
    ("number", None): lambda: sa.Float(precision=53),
    ("integer", None): sa.BigInteger,
    ("boolean", None): sa.Boolean,
    ("array", None): JSONB,
    ("object", None): JSONB,
}


def map_column(col_def: ColumnDef) -> sa.Column:
    """Convert a ColumnDef to a SQLAlchemy Column."""
    key = (col_def.json_type, col_def.format)
    type_factory = _TYPE_MAP.get(key)

    if type_factory is None:
        # Fall back to base type without format
        type_factory = _TYPE_MAP.get((col_def.json_type, None), sa.Text)

    sa_type = type_factory()

    return sa.Column(
        col_def.name,
        sa_type,
        nullable=not col_def.required,
    )
