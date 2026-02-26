"""consumer_group_delivery

Create deliveries table for per-consumer-group tracking.
Drop delivery columns from events table (becomes append-only log).
No data migration needed — pre-launch.

Revision ID: consumer_group_delivery
Revises: add_hooks_and_feature_tables
Create Date: 2026-02-26

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "consumer_group_delivery"
down_revision: Union[str, Sequence[str], None] = "add_hooks_and_feature_tables"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create deliveries table, drop delivery columns from events."""
    # 1. Create deliveries table
    op.create_table(
        "deliveries",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("event_id", sa.String(), sa.ForeignKey("events.id"), nullable=False),
        sa.Column("consumer_group", sa.String(128), nullable=False),
        sa.Column("status", sa.String(32), nullable=False, server_default=sa.text("'pending'")),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_error", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("event_id", "consumer_group", name="uq_delivery_event_consumer"),
    )

    # Deliveries indexes
    op.create_index(
        "idx_deliveries_claim",
        "deliveries",
        ["consumer_group", "status", "event_id"],
        postgresql_where=sa.text("status IN ('pending', 'claimed')"),
    )
    op.create_index("idx_deliveries_event", "deliveries", ["event_id"])
    op.create_index(
        "idx_deliveries_stale",
        "deliveries",
        ["claimed_at"],
        postgresql_where=sa.text("status = 'claimed'"),
    )
    op.create_index(
        "idx_deliveries_failed",
        "deliveries",
        ["consumer_group", "retry_count"],
        postgresql_where=sa.text("status = 'failed'"),
    )

    # 2. Drop delivery-related indexes from events
    op.drop_index("idx_events_failed", table_name="events")
    op.drop_index("idx_events_stale_claims", table_name="events")
    op.drop_index("idx_events_claim", table_name="events")
    op.drop_index("idx_events_delivery_status", table_name="events")

    # 3. Drop delivery columns from events (becomes append-only)
    op.drop_column("events", "updated_at")
    op.drop_column("events", "claimed_at")
    op.drop_column("events", "retry_count")
    op.drop_column("events", "routing_key")
    op.drop_column("events", "delivery_error")
    op.drop_column("events", "delivered_at")
    op.drop_column("events", "delivery_status")


def downgrade() -> None:
    """Restore delivery columns to events, drop deliveries table."""
    # Restore columns
    op.add_column(
        "events",
        sa.Column("delivery_status", sa.String(32), nullable=False, server_default="pending"),
    )
    op.add_column("events", sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("events", sa.Column("delivery_error", sa.Text(), nullable=True))
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

    # Restore indexes
    op.create_index("idx_events_delivery_status", "events", ["delivery_status"])
    op.create_index(
        "idx_events_claim",
        "events",
        ["delivery_status", "event_type", "routing_key", "created_at"],
        postgresql_where=sa.text("delivery_status IN ('pending', 'claimed')"),
    )
    op.create_index(
        "idx_events_stale_claims",
        "events",
        ["claimed_at"],
        postgresql_where=sa.text("delivery_status = 'claimed'"),
    )
    op.create_index(
        "idx_events_failed",
        "events",
        ["event_type", "created_at"],
        postgresql_where=sa.text("delivery_status = 'failed'"),
    )

    # Drop deliveries table
    op.drop_index("idx_deliveries_failed", table_name="deliveries")
    op.drop_index("idx_deliveries_stale", table_name="deliveries")
    op.drop_index("idx_deliveries_event", table_name="deliveries")
    op.drop_index("idx_deliveries_claim", table_name="deliveries")
    op.drop_table("deliveries")
