"""drop hook_releases (name, digest) unique constraint

Hook release idempotency changes from digest-only to definition-equality
against the live release (#217): a config-only redeploy now mints a new
release with the same digest, so the digest uniqueness constraint must go.
Mirrors the ingester registry, which shipped without the constraint.

Safe on populated tables: dropping a unique constraint is metadata-only.

Revision ID: b47f9c2e8a31
Revises: e1a7d20c4f9b
Create Date: 2026-08-15

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b47f9c2e8a31"
down_revision: Union[str, Sequence[str], None] = "e1a7d20c4f9b"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.drop_constraint("uq_hook_releases_hook_digest", "hook_releases", type_="unique")


def downgrade() -> None:
    """Downgrade schema.

    NB: recreating the constraint fails if any hook has minted multiple
    releases with one digest since upgrading — expected, and correct: those
    rows are legitimate under the new semantics.
    """
    op.create_unique_constraint(
        "uq_hook_releases_hook_digest", "hook_releases", ["hook_name", "digest"]
    )
