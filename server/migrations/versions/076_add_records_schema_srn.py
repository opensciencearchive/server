"""076_add_records_schema_id

Add ``records.schema_id`` + ``records.schema_version`` so a Record's typed
linkage is first-class (FR-008).

Greenfield only: no backfill from the linked convention. If this runs
against a populated ``records`` table it fails at ``SET NOT NULL`` with a
clear constraint error, which is the correct signal that the data predates
this schema.

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
    op.alter_column("records", "schema_id", nullable=False)
    op.alter_column("records", "schema_version", nullable=False)
    op.create_index("idx_records_schema_id", "records", ["schema_id"])


def downgrade() -> None:
    op.drop_index("idx_records_schema_id", table_name="records")
    op.drop_column("records", "schema_version")
    op.drop_column("records", "schema_id")
