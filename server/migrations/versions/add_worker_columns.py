"""add_worker_columns

Add columns and indexes to events table for pull-based worker architecture.

Revision ID: add_worker_columns
Revises: 0d9fbacf8e58
Create Date: 2026-02-02

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_worker_columns"
down_revision: Union[str, Sequence[str], None] = "0d9fbacf8e58"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add worker columns to events table."""
    # Add new columns for pull-based claiming
    op.add_column("events", sa.Column("routing_key", sa.String(255), nullable=True))
    op.add_column(
        "events", sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0")
    )
    op.add_column("events", sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column(
        "events",
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
    )

    # Create partial index for efficient claiming query
    # Covers: status=pending/claimed, event_type, routing_key, created_at
    op.create_index(
        "idx_events_claim",
        "events",
        ["delivery_status", "event_type", "routing_key", "created_at"],
        postgresql_where=sa.text("delivery_status IN ('pending', 'claimed')"),
    )

    # Create partial index for stale claim detection
    op.create_index(
        "idx_events_stale_claims",
        "events",
        ["claimed_at"],
        postgresql_where=sa.text("delivery_status = 'claimed'"),
    )

    # Create partial index for failed event queries
    op.create_index(
        "idx_events_failed",
        "events",
        ["event_type", "created_at"],
        postgresql_where=sa.text("delivery_status = 'failed'"),
    )


def downgrade() -> None:
    """Remove worker columns from events table."""
    op.drop_index("idx_events_failed", table_name="events")
    op.drop_index("idx_events_stale_claims", table_name="events")
    op.drop_index("idx_events_claim", table_name="events")
    op.drop_column("events", "updated_at")
    op.drop_column("events", "claimed_at")
    op.drop_column("events", "retry_count")
    op.drop_column("events", "routing_key")
