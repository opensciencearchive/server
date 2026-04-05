"""add_deliver_after_and_batches_failed

Add deliver_after column to deliveries table for explicit backoff scheduling.
Add batches_failed column to ingest_runs table for batch failure accounting.

Revision ID: add_deliver_after
Revises: add_ingest_runs
Create Date: 2026-04-04

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_deliver_after"
down_revision: Union[str, Sequence[str], None] = "add_ingest_runs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "deliveries",
        sa.Column("deliver_after", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "idx_deliveries_deliver_after",
        "deliveries",
        ["deliver_after"],
        postgresql_where=sa.text("status = 'pending'"),
    )

    op.add_column(
        "ingest_runs",
        sa.Column("batches_failed", sa.Integer, nullable=False, server_default=sa.text("0")),
    )


def downgrade() -> None:
    op.drop_column("ingest_runs", "batches_failed")
    op.drop_index("idx_deliveries_deliver_after", table_name="deliveries")
    op.drop_column("deliveries", "deliver_after")
