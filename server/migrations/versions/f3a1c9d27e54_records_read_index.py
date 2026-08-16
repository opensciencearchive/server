"""Composite read index on records; drop the subsumed schema_id index.

``records (schema_id, schema_version, published_at, srn)`` serves the default
table read — schema equality prefix + (published_at, srn) ordering — as one
(backward) index range scan, including the row-value keyset predicate (#219
phase 3). ``idx_records_schema_id`` is its left prefix and is dropped;
``idx_records_published_at`` stays (used by count_this_month's month filter).

Built ``CONCURRENTLY``: live archives run this migration on a records table
serving traffic, so the build must not take a table lock. CONCURRENTLY cannot
run inside a transaction block, hence the autocommit block.

Revision ID: f3a1c9d27e54
Revises: b47f9c2e8a31
Create Date: 2026-08-15
"""

from alembic import op

# revision identifiers, used by Alembic.
revision = "f3a1c9d27e54"
down_revision = "b47f9c2e8a31"
branch_labels = None
depends_on = None

_INDEX = "idx_records_schema_version_published"


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.create_index(
            _INDEX,
            "records",
            ["schema_id", "schema_version", "published_at", "srn"],
            postgresql_concurrently=True,
        )
    op.drop_index("idx_records_schema_id", table_name="records")


def downgrade() -> None:
    op.create_index("idx_records_schema_id", "records", ["schema_id"])
    with op.get_context().autocommit_block():
        op.drop_index(_INDEX, table_name="records", postgresql_concurrently=True)
