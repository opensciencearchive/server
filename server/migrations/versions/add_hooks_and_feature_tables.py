"""add_hooks_and_feature_tables

Replace conventions.validator_refs with hooks JSON column.
Add source JSON column to conventions.
Add feature_tables catalog table for tracking dynamically created feature tables.

Revision ID: add_hooks_and_feature_tables
Revises: add_deposition_tables
Create Date: 2026-02-20

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_hooks_and_feature_tables"
down_revision: Union[str, Sequence[str], None] = "add_deposition_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Replace validator_refs with hooks; add source; add feature_tables catalog."""
    # CONVENTIONS: replace validator_refs with hooks, add source
    op.add_column(
        "conventions",
        sa.Column("hooks", sa.JSON(), nullable=False, server_default="[]"),
    )
    op.add_column(
        "conventions",
        sa.Column("source", sa.JSON(), nullable=True),
    )
    op.drop_column("conventions", "validator_refs")

    # FEATURE_TABLES: catalog for dynamically created feature tables
    op.create_table(
        "feature_tables",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("hook_name", sa.String(), nullable=False),
        sa.Column("pg_table", sa.String(), nullable=False),
        sa.Column("feature_schema", sa.JSON(), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("hook_name", name="uq_feature_tables_hook_name"),
    )


def downgrade() -> None:
    """Reverse: drop feature_tables, restore validator_refs, drop source."""
    op.drop_table("feature_tables")

    op.drop_column("conventions", "source")
    op.add_column(
        "conventions",
        sa.Column("validator_refs", sa.JSON(), nullable=False, server_default="[]"),
    )
    op.drop_column("conventions", "hooks")
