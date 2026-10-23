"""``ingest_runs.records_sourced`` — the counter the run limit actually counts.

The record limit was derived from ``batches_ingested * batch_size``, which
assumes every batch filled. Batches underfill routinely (the SDK entrypoint
skips records whose files exceed ``max_file_mb``, and the last batch of a
limited run is capped short), so the estimate ran ahead of reality and ended
limited runs early — ``--limit 250`` could yield 170 records with nothing in
the log to explain the shortfall (#227).

Existing rows backfill to the old estimate rather than 0: it is the only
figure available for a historical run, and seeding 0 would let a resumed
limited run source its whole limit again. Capped at the run's own limit so a
resumed run cannot overshoot. Live runs are unaffected in practice — the
counter advances from the next batch they source.

Revision ID: d2f8a71c4b60
Revises: a8c4e6f19b02
Create Date: 2026-10-21
"""

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "d2f8a71c4b60"
down_revision = "a8c4e6f19b02"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "ingest_runs",
        sa.Column(
            "records_sourced",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.execute(
        """
        UPDATE ingest_runs
           SET records_sourced = LEAST(
                   batches_ingested * batch_size,
                   COALESCE(record_limit, batches_ingested * batch_size)
               )
         WHERE batches_ingested > 0
        """
    )


def downgrade() -> None:
    op.drop_column("ingest_runs", "records_sourced")
