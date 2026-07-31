"""add instance_statistics

Revision ID: 44a8e3799b97
Revises: c6d9f4c0c3ab
Create Date: 2026-07-31 10:27:41.504686

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "44a8e3799b97"
down_revision: Union[str, Sequence[str], None] = "c6d9f4c0c3ab"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema.

    Idempotent create: a database stamped at ``c6d9f4c0c3ab`` may already hold
    ``instance_statistics`` if it ran an earlier form of the initial migration
    that inlined the table. Skip the create in that case so the corrective
    revision can't fail with a duplicate-relation error.
    """
    if sa.inspect(op.get_bind()).has_table("instance_statistics"):
        return
    op.create_table(
        "instance_statistics",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("storage_bytes", sa.BigInteger(), nullable=False),
        sa.Column("feature_rows", sa.BigInteger(), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    if sa.inspect(op.get_bind()).has_table("instance_statistics"):
        op.drop_table("instance_statistics")
