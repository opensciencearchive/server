"""076_add_records_schema_srn

Add a ``records.schema_srn`` column so Record linkage to its typed metadata
shape is first-class (FR-008). Backfill from the linked convention's
``schema_srn`` before tightening to NOT NULL.

Greenfield deployments with no records will skip the backfill (the UPDATE is
a no-op) and go straight to NOT NULL.

Revision ID: 076_records_schema_srn
Revises: 076_metadata_catalog
Create Date: 2026-04-19

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "076_records_schema_srn"
down_revision: Union[str, Sequence[str], None] = "076_metadata_catalog"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("records", sa.Column("schema_srn", sa.Text(), nullable=True))

    op.execute(
        """
        UPDATE records r
        SET schema_srn = c.schema_srn
        FROM conventions c
        WHERE c.srn = r.convention_srn
          AND r.schema_srn IS NULL
        """
    )

    op.alter_column("records", "schema_srn", nullable=False)
    op.create_index("idx_records_schema_srn", "records", ["schema_srn"])


def downgrade() -> None:
    op.drop_index("idx_records_schema_srn", table_name="records")
    op.drop_column("records", "schema_srn")
