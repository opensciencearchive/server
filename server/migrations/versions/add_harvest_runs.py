"""add_ingest_runs

Add ingest_runs table for bulk ingestion tracking.

Revision ID: add_harvest_runs
Revises: source_agnostic_records
Create Date: 2026-03-25

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_harvest_runs"
down_revision: Union[str, Sequence[str], None] = "source_agnostic_records"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ingest_runs",
        sa.Column("srn", sa.String(), primary_key=True),
        sa.Column(
            "convention_srn",
            sa.String(),
            sa.ForeignKey("conventions.srn"),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(32),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "source_finished",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("false"),
        ),
        sa.Column(
            "batches_sourced",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "batches_completed",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "published_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "batch_size",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("1000"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed')",
            name="ingest_runs_status_check",
        ),
    )

    op.create_index(
        "idx_ingest_runs_convention",
        "ingest_runs",
        ["convention_srn"],
    )
    op.create_index(
        "idx_ingest_runs_status",
        "ingest_runs",
        ["status"],
    )


def downgrade() -> None:
    op.drop_index("idx_ingest_runs_status", table_name="ingest_runs")
    op.drop_index("idx_ingest_runs_convention", table_name="ingest_runs")
    op.drop_table("ingest_runs")
