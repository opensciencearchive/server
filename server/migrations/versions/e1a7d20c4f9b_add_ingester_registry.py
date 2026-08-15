"""add ingester registry: identities, releases, run snapshot

Ingester identity + immutable versioned releases (#180 §1), mirroring the hook
registry, plus ``ingest_runs.release_id`` — the release a run resolved at start,
snapshotted for provenance. Incremental revision (live archives upgrade in
place).

Greenfield provenance: ``release_id`` is NOT NULL — every run is traceable to
the exact code that fetched its records. Pre-registry runs carry no release to
point at, so the upgrade clears ``ingest_runs``: runs are operational state and
re-runnable (re-ingestion re-creates records with full provenance), not archival
data.

Revision ID: e1a7d20c4f9b
Revises: 44a8e3799b97
Create Date: 2026-08-14

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "e1a7d20c4f9b"
down_revision: Union[str, Sequence[str], None] = "44a8e3799b97"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "ingesters",
        sa.Column("name", sa.String(length=40), nullable=False),
        sa.Column("schema_id", sa.String(), nullable=False),
        sa.Column("live_release_id", sa.UUID(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("name"),
    )
    op.create_index("idx_ingesters_schema_id", "ingesters", ["schema_id"], unique=False)
    op.create_table(
        "ingester_releases",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("ingester_name", sa.String(length=40), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("image", sa.Text(), nullable=False),
        sa.Column("digest", sa.Text(), nullable=False),
        sa.Column(
            "config",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'"),
            nullable=False,
        ),
        sa.Column("limits", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("source_ref", sa.Text(), nullable=False),
        sa.Column("built_by", sa.Text(), nullable=True),
        sa.Column("built_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["ingester_name"], ["ingesters.name"]),
        sa.PrimaryKeyConstraint("id"),
        # No (name, digest) unique constraint: a config-only redeploy mints a
        # new release with the same digest (idempotency is definition-equality
        # against the live release, decided in the adapter).
        sa.UniqueConstraint(
            "ingester_name", "version", name="uq_ingester_releases_ingester_version"
        ),
    )
    op.create_index(
        "idx_ingester_releases_ingester_version",
        "ingester_releases",
        ["ingester_name", sa.literal_column("version DESC")],
        unique=False,
    )
    # Same circular-dependency break as hooks.live_release_id in initial_schema.
    op.create_foreign_key(
        "fk_ingesters_live_release_id",
        "ingesters",
        "ingester_releases",
        ["live_release_id"],
        ["id"],
        deferrable=True,
        initially="DEFERRED",
    )
    # Pre-registry runs have no release to point at; runs are re-runnable
    # operational state, so clear them rather than carry a nullable column.
    op.execute("DELETE FROM ingest_runs")
    op.add_column(
        "ingest_runs",
        sa.Column("release_id", sa.UUID(), nullable=False),
    )
    op.create_foreign_key(
        "fk_ingest_runs_release_id",
        "ingest_runs",
        "ingester_releases",
        ["release_id"],
        ["id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_constraint("fk_ingest_runs_release_id", "ingest_runs", type_="foreignkey")
    op.drop_column("ingest_runs", "release_id")
    op.drop_constraint("fk_ingesters_live_release_id", "ingesters", type_="foreignkey")
    op.drop_index("idx_ingester_releases_ingester_version", table_name="ingester_releases")
    op.drop_table("ingester_releases")
    op.drop_index("idx_ingesters_schema_id", table_name="ingesters")
    op.drop_table("ingesters")
