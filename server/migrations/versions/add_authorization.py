"""add_authorization

Add role_assignments table and owner_id column to depositions.

Revision ID: add_authorization
Revises: add_auth_tables
Create Date: 2026-02-06

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_authorization"
down_revision: Union[str, Sequence[str], None] = "add_auth_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add authorization tables and columns."""
    # ROLE ASSIGNMENTS TABLE
    op.create_table(
        "role_assignments",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("user_id", sa.String(), nullable=False),
        sa.Column("role", sa.String(32), nullable=False),
        sa.Column("assigned_by", sa.String(), nullable=False),
        sa.Column("assigned_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["assigned_by"],
            ["users.id"],
        ),
        sa.UniqueConstraint("user_id", "role", name="uq_role_assignments_user_role"),
    )
    op.create_index("ix_role_assignments_user_id", "role_assignments", ["user_id"])

    # ADD owner_id TO DEPOSITIONS (nullable initially for existing data)
    op.add_column(
        "depositions",
        sa.Column("owner_id", sa.String(), nullable=True),
    )
    op.create_foreign_key(
        "fk_depositions_owner_id",
        "depositions",
        "users",
        ["owner_id"],
        ["id"],
    )
    op.create_index("idx_depositions_owner_id", "depositions", ["owner_id"])


def downgrade() -> None:
    """Remove authorization tables and columns."""
    # DEPOSITIONS owner_id
    op.drop_index("idx_depositions_owner_id", table_name="depositions")
    op.drop_constraint("fk_depositions_owner_id", "depositions", type_="foreignkey")
    op.drop_column("depositions", "owner_id")

    # ROLE ASSIGNMENTS
    op.drop_index("ix_role_assignments_user_id", table_name="role_assignments")
    op.drop_table("role_assignments")
