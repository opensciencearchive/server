"""initial_tables

Revision ID: 0d9fbacf8e58
Revises:
Create Date: 2025-11-28 01:22:35.013560

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0d9fbacf8e58"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    # DEPOSITIONS
    op.create_table(
        "depositions",
        sa.Column("srn", sa.String(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("provenance", sa.JSON(), nullable=False),
        sa.Column("files", sa.JSON(), nullable=False),
        sa.Column("record_id", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("srn"),
    )
    op.create_index("idx_depositions_record_id", "depositions", ["record_id"])

    # VALIDATION RUNS
    op.create_table(
        "validation_runs",
        sa.Column("srn", sa.String(), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("results", sa.JSON(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("srn"),
    )
    op.create_index("idx_validation_runs_expires_at", "validation_runs", ["expires_at"])

    # RECORDS
    op.create_table(
        "records",
        sa.Column("srn", sa.String(), nullable=False),
        sa.Column("deposition_srn", sa.String(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("indexes", sa.JSON(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("srn"),
    )
    op.create_index("idx_records_deposition_srn", "records", ["deposition_srn"])
    op.create_index("idx_records_published_at", "records", ["published_at"])

    # EVENTS (Outbox)
    op.create_table(
        "events",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("event_type", sa.String(128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("delivery_status", sa.String(32), nullable=False),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivery_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_events_type_created",
        "events",
        ["event_type", sa.text("created_at DESC")],
    )
    op.create_index("idx_events_delivery_status", "events", ["delivery_status"])


def downgrade() -> None:
    """Downgrade schema."""
    # EVENTS
    op.drop_index("idx_events_delivery_status", table_name="events")
    op.drop_index("idx_events_type_created", table_name="events")
    op.drop_table("events")

    # RECORDS
    op.drop_index("idx_records_published_at", table_name="records")
    op.drop_index("idx_records_deposition_srn", table_name="records")
    op.drop_table("records")

    # VALIDATION RUNS
    op.drop_index("idx_validation_runs_expires_at", table_name="validation_runs")
    op.drop_table("validation_runs")

    # DEPOSITIONS
    op.drop_index("idx_depositions_record_id", table_name="depositions")
    op.drop_table("depositions")
