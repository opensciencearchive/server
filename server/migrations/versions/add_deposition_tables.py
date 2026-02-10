"""add_deposition_tables

Add ontologies, ontology_terms, schemas, conventions tables.
Alter depositions: add convention_srn, drop provenance.

Revision ID: add_deposition_tables
Revises: add_authorization
Create Date: 2026-02-08

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "add_deposition_tables"
down_revision: Union[str, Sequence[str], None] = "add_authorization"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add semantics/convention tables and update depositions."""
    # ONTOLOGIES
    op.create_table(
        "ontologies",
        sa.Column("srn", sa.String(), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("srn"),
    )

    # ONTOLOGY TERMS
    op.create_table(
        "ontology_terms",
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("ontology_srn", sa.String(), nullable=False),
        sa.Column("term_id", sa.String(255), nullable=False),
        sa.Column("label", sa.String(255), nullable=False),
        sa.Column("synonyms", sa.JSON(), nullable=False),
        sa.Column("parent_ids", sa.JSON(), nullable=False),
        sa.Column("definition", sa.Text(), nullable=True),
        sa.Column("deprecated", sa.Boolean(), nullable=False, server_default="false"),
        sa.PrimaryKeyConstraint("id"),
        sa.ForeignKeyConstraint(
            ["ontology_srn"],
            ["ontologies.srn"],
            ondelete="CASCADE",
        ),
        sa.UniqueConstraint("ontology_srn", "term_id", name="uq_ontology_term"),
    )
    op.create_index("idx_ontology_terms_ontology_srn", "ontology_terms", ["ontology_srn"])

    # SCHEMAS
    op.create_table(
        "schemas",
        sa.Column("srn", sa.String(), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("fields", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("srn"),
    )

    # CONVENTIONS
    op.create_table(
        "conventions",
        sa.Column("srn", sa.String(), nullable=False),
        sa.Column("title", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("schema_srn", sa.String(), nullable=False),
        sa.Column("file_requirements", sa.JSON(), nullable=False),
        sa.Column("validator_refs", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("srn"),
    )

    # ALTER DEPOSITIONS: add convention_srn, drop provenance
    op.add_column(
        "depositions",
        sa.Column("convention_srn", sa.String(), nullable=True),
    )
    op.drop_column("depositions", "provenance")


def downgrade() -> None:
    """Reverse: restore depositions, drop new tables."""
    # DEPOSITIONS: re-add provenance, drop convention_srn
    op.add_column(
        "depositions",
        sa.Column("provenance", sa.JSON(), nullable=False, server_default="{}"),
    )
    op.drop_column("depositions", "convention_srn")

    # CONVENTIONS
    op.drop_table("conventions")

    # SCHEMAS
    op.drop_table("schemas")

    # ONTOLOGY TERMS
    op.drop_index("idx_ontology_terms_ontology_srn", table_name="ontology_terms")
    op.drop_table("ontology_terms")

    # ONTOLOGIES
    op.drop_table("ontologies")
