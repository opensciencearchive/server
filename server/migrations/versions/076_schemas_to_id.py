"""076_schemas_to_id

Replace URN-keyed ``schemas`` and ``conventions`` columns with short-form
``(id, version)`` pairs. After this migration, internal code works entirely
in ``SchemaId``; full URNs are reserved for federation edges.

Changes:
- ``schemas.srn`` → ``schemas.id`` + ``schemas.version``. Composite PK.
- ``conventions.schema_srn`` → ``conventions.schema_id`` + ``conventions.schema_version``.

Greenfield only: no backfill from the old URN columns. If this runs against
a populated DB it fails at ``SET NOT NULL`` with a clear constraint error,
which is the correct signal that the data predates this schema.

Revision ID: 076_schemas_to_id
Revises: 076_metadata_catalog
Create Date: 2026-04-20

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "076_schemas_to_id"
down_revision: Union[str, Sequence[str], None] = "076_metadata_catalog"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # schemas: drop old SRN PK, add id + version, recompose PK.
    op.add_column("schemas", sa.Column("id", sa.String(), nullable=True))
    op.add_column("schemas", sa.Column("version", sa.String(), nullable=True))
    op.alter_column("schemas", "id", nullable=False)
    op.alter_column("schemas", "version", nullable=False)
    op.drop_constraint("schemas_pkey", "schemas", type_="primary")
    op.drop_column("schemas", "srn")
    op.create_primary_key("schemas_pkey", "schemas", ["id", "version"])
    op.create_index("idx_schemas_id", "schemas", ["id"])

    # conventions: split schema_srn into schema_id + schema_version.
    op.add_column("conventions", sa.Column("schema_id", sa.String(), nullable=True))
    op.add_column("conventions", sa.Column("schema_version", sa.String(), nullable=True))
    op.alter_column("conventions", "schema_id", nullable=False)
    op.alter_column("conventions", "schema_version", nullable=False)
    op.drop_column("conventions", "schema_srn")


def downgrade() -> None:
    # conventions back to schema_srn
    op.add_column("conventions", sa.Column("schema_srn", sa.String(), nullable=True))
    op.alter_column("conventions", "schema_srn", nullable=False)
    op.drop_column("conventions", "schema_version")
    op.drop_column("conventions", "schema_id")

    # schemas back to srn
    op.drop_index("idx_schemas_id", table_name="schemas")
    op.drop_constraint("schemas_pkey", "schemas", type_="primary")
    op.add_column("schemas", sa.Column("srn", sa.String(), nullable=True))
    op.alter_column("schemas", "srn", nullable=False)
    op.create_primary_key("schemas_pkey", "schemas", ["srn"])
    op.drop_column("schemas", "version")
    op.drop_column("schemas", "id")
