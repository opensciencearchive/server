"""``table_statistics`` — lockstep counts, backfilled once from the data.

Creates the per-(schema version, table) count state and populates it from
COUNT(*) / COUNT(DISTINCT record_srn) group-bys over the existing tables.
This backfill and the admin verifier are the only sanctioned whole-table
counting after #219; from here on the writing adapters maintain the counts
transactionally (osa/infrastructure/persistence/statistics_upsert.py).

Feature-table names come from the ``feature_tables`` catalog and are
re-validated against the strict identifier pattern before interpolation —
never string-built from user input.

Revision ID: a8c4e6f19b02
Revises: f3a1c9d27e54
Create Date: 2026-08-15
"""

import re

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "a8c4e6f19b02"
down_revision = "f3a1c9d27e54"
branch_labels = None
depends_on = None

_SAFE_IDENT = re.compile(r"^[a-z][a-z0-9_]{0,62}$")


def upgrade() -> None:
    op.create_table(
        "table_statistics",
        sa.Column("schema_id", sa.Text(), primary_key=True),
        sa.Column("schema_version", sa.Text(), primary_key=True),
        sa.Column("table_name", sa.Text(), primary_key=True),
        sa.Column("row_count", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("records_covered", sa.BigInteger(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )

    conn = op.get_bind()
    # Records: one row per schema version present in the data.
    conn.execute(
        sa.text(
            """
            INSERT INTO table_statistics
                (schema_id, schema_version, table_name, row_count, records_covered, updated_at)
            SELECT schema_id, schema_version, 'records', count(*), NULL, now()
            FROM records
            GROUP BY schema_id, schema_version
            """
        )
    )
    # Features: every registered feature table, scoped per schema via the records join.
    tables = conn.execute(sa.text("SELECT hook_name, pg_table FROM feature_tables")).fetchall()
    for hook_name, pg_table in tables:
        if not _SAFE_IDENT.match(pg_table):
            continue
        conn.execute(
            sa.text(
                f"""
                INSERT INTO table_statistics
                    (schema_id, schema_version, table_name, row_count,
                     records_covered, updated_at)
                SELECT r.schema_id, r.schema_version, :hook, count(ft.id),
                       count(DISTINCT ft.record_srn), now()
                FROM features."{pg_table}" ft
                JOIN records r ON r.srn = ft.record_srn
                GROUP BY r.schema_id, r.schema_version
                """
            ),
            {"hook": hook_name},
        )


def downgrade() -> None:
    op.drop_table("table_statistics")
