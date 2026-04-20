"""076_add_metadata_schema_and_catalog

Create the ``metadata`` PostgreSQL schema and the ``public.metadata_tables``
catalog table. Dynamic per-schema metadata tables will live inside the
``metadata`` schema; the catalog indexes them by schema identity+major.

Revision ID: 076_metadata_catalog
Revises: add_deliver_after
Create Date: 2026-04-19

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers, used by Alembic.
revision: str = "076_metadata_catalog"
down_revision: Union[str, Sequence[str], None] = "add_deliver_after"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute('CREATE SCHEMA IF NOT EXISTS "metadata"')

    op.create_table(
        "metadata_tables",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("schema_identity", sa.Text(), nullable=False),
        sa.Column("schema_slug", sa.Text(), nullable=False),
        sa.Column("schema_major", sa.Integer(), nullable=False),
        sa.Column("schema_versions", JSONB(), nullable=False),
        sa.Column("pg_table", sa.Text(), nullable=False),
        sa.Column("metadata_schema", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint(
            "schema_identity",
            "schema_major",
            name="uq_metadata_tables_identity_major",
        ),
        sa.UniqueConstraint("pg_table", name="uq_metadata_tables_pg_table"),
    )


def downgrade() -> None:
    op.drop_table("metadata_tables")
    op.execute('DROP SCHEMA IF EXISTS "metadata" CASCADE')
