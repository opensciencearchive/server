"""source_agnostic_records

Replace deposition_srn + indexes with source (JSONB) + convention_srn.
No data migration needed — no production data exists.

Revision ID: source_agnostic_records
Revises: add_device_authorizations
Create Date: 2026-03-24

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "source_agnostic_records"
down_revision: Union[str, Sequence[str], None] = "add_device_authorizations"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop old indexes
    op.drop_index("idx_records_deposition_srn", table_name="records")

    # Drop old columns
    op.drop_column("records", "deposition_srn")
    op.drop_column("records", "indexes")

    # Add new columns
    op.add_column(
        "records",
        sa.Column("convention_srn", sa.Text(), nullable=False),
    )
    op.add_column(
        "records",
        sa.Column("source", sa.dialects.postgresql.JSONB(), nullable=False),
    )

    # Add new indexes
    op.create_index(
        "idx_records_convention_srn",
        "records",
        ["convention_srn"],
    )
    op.create_index(
        "uq_records_source",
        "records",
        [
            sa.text("(source->>'type')"),
            sa.text("(source->>'id')"),
        ],
        unique=True,
    )


def downgrade() -> None:
    # Drop new indexes
    op.drop_index("uq_records_source", table_name="records")
    op.drop_index("idx_records_convention_srn", table_name="records")

    # Drop new columns
    op.drop_column("records", "source")
    op.drop_column("records", "convention_srn")

    # Re-add old columns
    op.add_column(
        "records",
        sa.Column("deposition_srn", sa.String(), nullable=False),
    )
    op.add_column(
        "records",
        sa.Column("indexes", sa.JSON(), nullable=False),
    )

    # Re-add old index
    op.create_index(
        "idx_records_deposition_srn",
        "records",
        ["deposition_srn"],
    )
