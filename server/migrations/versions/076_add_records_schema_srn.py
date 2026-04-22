"""076_add_records_schema_id

Add ``records.schema_id`` + ``records.schema_version`` so a Record's typed
linkage is first-class (FR-008). Backfill from the linked convention's
``schema_id`` / ``schema_version`` columns, then tighten to NOT NULL.

Greenfield deployments with no records will skip the backfill and go straight
to NOT NULL.

Revision ID: 076_records_schema_srn
Revises: 076_schemas_to_id
Create Date: 2026-04-19

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "076_records_schema_srn"
down_revision: Union[str, Sequence[str], None] = "076_schemas_to_id"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("records", sa.Column("schema_id", sa.Text(), nullable=True))
    op.add_column("records", sa.Column("schema_version", sa.Text(), nullable=True))

    # Backfill from the owning convention's schema_id/schema_version
    # (populated by ``076_schemas_to_id`` which ran just before this).
    op.execute(
        """
        UPDATE records r
        SET
            schema_id = c.schema_id,
            schema_version = c.schema_version
        FROM conventions c
        WHERE c.srn = r.convention_srn
          AND r.schema_id IS NULL
        """
    )

    op.alter_column("records", "schema_id", nullable=False)
    op.alter_column("records", "schema_version", nullable=False)
    op.create_index("idx_records_schema_id", "records", ["schema_id"])


def downgrade() -> None:
    op.drop_index("idx_records_schema_id", table_name="records")
    op.drop_column("records", "schema_version")
    op.drop_column("records", "schema_id")
