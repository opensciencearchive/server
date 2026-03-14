"""add_device_authorizations

Create device_authorizations table for OAuth device flow.

Revision ID: add_device_authorizations
Revises: consumer_group_delivery
Create Date: 2026-03-13

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_device_authorizations"
down_revision: Union[str, Sequence[str], None] = "consumer_group_delivery"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create device_authorizations table."""
    op.create_table(
        "device_authorizations",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("device_code", sa.String(64), nullable=False),
        sa.Column("user_code", sa.String(8), nullable=False),
        sa.Column(
            "status",
            sa.String(20),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "user_id",
            sa.String(),
            sa.ForeignKey("users.id"),
            nullable=True,
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )

    op.create_unique_constraint(
        "uq_device_auth_device_code",
        "device_authorizations",
        ["device_code"],
    )
    op.create_unique_constraint(
        "uq_device_auth_user_code",
        "device_authorizations",
        ["user_code"],
    )
    op.create_index(
        "ix_device_auth_status_expires",
        "device_authorizations",
        ["status", "expires_at"],
    )


def downgrade() -> None:
    """Drop device_authorizations table."""
    op.drop_index("ix_device_auth_status_expires", table_name="device_authorizations")
    op.drop_table("device_authorizations")
