"""076_add_feature_tables_record_srn_fks

For each row currently registered in the ``public.feature_tables`` catalog,
add a foreign-key constraint on ``features.<hook>.record_srn`` referencing
``records.srn`` with ``ON DELETE CASCADE``. Bundles GitHub #75.

Idempotent: skips any hook whose FK is already present (detected by naming
convention). No-op on greenfield deployments where the catalog is empty.

Revision ID: 076_feature_fks
Revises: 076_records_schema_srn
Create Date: 2026-04-19

"""

import re
from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "076_feature_fks"
down_revision: Union[str, Sequence[str], None] = "076_records_schema_srn"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


FK_NAME_TEMPLATE = "fk_features_{hook}_record_srn"

# Defense-in-depth: hook names read from ``feature_tables`` are interpolated
# into raw DDL below. Application code constrains hooks to this shape at write
# time, but the migration should not trust that invariant — a stray ``"`` in a
# stored name would break out of quoting. Mirrors the ``_safe_ident`` check in
# ``osa.infrastructure.persistence.metadata_store``.
_PG_IDENT_RE = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def _safe_ident(name: str) -> str:
    if not _PG_IDENT_RE.match(name):
        raise ValueError(f"Refusing to interpolate unsafe PG identifier {name!r} into DDL")
    return name


def upgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(_select_hooks()).fetchall()

    for row in rows:
        hook = _safe_ident(row[0])
        fk_name = _safe_ident(FK_NAME_TEMPLATE.format(hook=hook))
        exists = conn.execute(_check_constraint(fk_name)).scalar()
        if exists:
            continue

        conn.execute(_add_fk_sql(hook, fk_name))


def downgrade() -> None:
    conn = op.get_bind()
    rows = conn.execute(_select_hooks()).fetchall()
    for row in rows:
        hook = _safe_ident(row[0])
        fk_name = _safe_ident(FK_NAME_TEMPLATE.format(hook=hook))
        exists = conn.execute(_check_constraint(fk_name)).scalar()
        if not exists:
            continue
        conn.execute(_drop_fk_sql(hook, fk_name))


def _select_hooks():
    from sqlalchemy import text

    return text("SELECT hook_name FROM feature_tables")


def _check_constraint(fk_name: str):
    from sqlalchemy import text

    return text("SELECT 1 FROM pg_constraint WHERE conname = :fk_name").bindparams(fk_name=fk_name)


def _add_fk_sql(hook: str, fk_name: str):
    from sqlalchemy import text

    return text(
        f'ALTER TABLE features."{hook}" '
        f'ADD CONSTRAINT "{fk_name}" '
        f"FOREIGN KEY (record_srn) REFERENCES records(srn) ON DELETE CASCADE"
    )


def _drop_fk_sql(hook: str, fk_name: str):
    from sqlalchemy import text

    return text(f'ALTER TABLE features."{hook}" DROP CONSTRAINT "{fk_name}"')
