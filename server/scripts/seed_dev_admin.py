"""Seed the local-dev SUPERADMIN user.

Invoked by `scripts/entrypoint.sh` when `OSA_DEV_MODE=true`. Mirrors the
Supabase-style local affordance: pair the well-known dev JWT secret with a
deterministic admin user row, so a freshly-started local stack can be driven
without going through ORCiD OAuth.

The admin's `LinkedAccount` has `provider="local"`, `external_id="admin@osa.local"`
(or whatever `auth.admins.local` lists), exactly matching what the CLI puts
into its self-minted JWT (`provider`, `external_id`).

Standalone script — no `osa` package imports required, so it runs the same way
in any deployment that mounts the migrations and the venv. Idempotent: safe on
every startup.
"""

import asyncio
import os
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path

import yaml
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Deterministic UUIDv7 — fixed so re-seeding is a no-op and tests can rely on
# the admin user ID. UUIDv7 layout: timestamp || ver=7 || rand || var=10 || rand.
DEV_ADMIN_USER_ID = "00000000-0000-7000-8000-0000000000a1"

DEFAULT_LOCAL_PROVIDER = "local"
DEFAULT_LOCAL_EXTERNAL_IDS = ["admin@osa.local"]
DEFAULT_DISPLAY_NAME = "Local Dev Admin"
SUPERADMIN_ROLE = "SUPERADMIN"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_local_external_ids() -> list[str]:
    """Resolve admins.local from OSA_CONFIG_FILE (YAML) if present, else default.

    We read the YAML directly instead of constructing `Config` to keep this
    script standalone. The env-var override path (OSA_AUTH__ADMINS__LOCAL) is
    not supported here — uncommon for a list, and YAML config covers the
    realistic case.
    """
    config_file = os.environ.get("OSA_CONFIG_FILE")
    if not config_file:
        return DEFAULT_LOCAL_EXTERNAL_IDS

    path = Path(config_file)
    if not path.exists():
        return DEFAULT_LOCAL_EXTERNAL_IDS

    data = yaml.safe_load(path.read_text()) or {}
    locals_ = data.get("auth", {}).get("admins", {}).get("local")
    if not isinstance(locals_, list) or not locals_:
        return DEFAULT_LOCAL_EXTERNAL_IDS
    return [str(x) for x in locals_]


def _deterministic_identity_id(provider: str, external_id: str) -> str:
    """Stable UUID for the (provider, external_id) pair.

    Using uuid5 keeps re-runs idempotent without an extra SELECT per identity.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"osa:identity:{provider}:{external_id}"))


def _deterministic_role_assignment_id(user_id: str, role: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"osa:role:{user_id}:{role}"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def seed() -> None:
    db_url = os.environ.get("OSA_DATABASE__URL")
    if not db_url:
        print("seed_dev_admin: OSA_DATABASE__URL not set, skipping", file=sys.stderr)
        sys.exit(1)

    external_ids = _load_local_external_ids()
    engine = create_async_engine(db_url)
    now = datetime.now(UTC)
    system_user_id = "00000000-0000-0000-0000-000000000000"

    async with engine.begin() as conn:
        # 1a) Upsert the system user (FK target for role_assignments.assigned_by).
        # `ensure_system_user` runs in the server lifespan, but this script runs
        # in the entrypoint BEFORE uvicorn starts, so we can't rely on it.
        await conn.execute(
            text(
                "INSERT INTO users (id, display_name, created_at) "
                "VALUES (:id, :name, :created_at) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": system_user_id,
                "name": "System",
                "created_at": now,
            },
        )

        # 1b) Upsert dev admin user row (deterministic ID, idempotent)
        await conn.execute(
            text(
                "INSERT INTO users (id, display_name, created_at) "
                "VALUES (:id, :name, :created_at) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": DEV_ADMIN_USER_ID,
                "name": DEFAULT_DISPLAY_NAME,
                "created_at": now,
            },
        )

        for external_id in external_ids:
            # 2) Upsert linked account (identities table) for each configured ID
            identity_id = _deterministic_identity_id(DEFAULT_LOCAL_PROVIDER, external_id)
            await conn.execute(
                text(
                    "INSERT INTO identities (id, user_id, provider, external_id, metadata, created_at) "
                    "VALUES (:id, :user_id, :provider, :external_id, NULL, :created_at) "
                    "ON CONFLICT ON CONSTRAINT uq_identity_provider_external DO NOTHING"
                ),
                {
                    "id": identity_id,
                    "user_id": DEV_ADMIN_USER_ID,
                    "provider": DEFAULT_LOCAL_PROVIDER,
                    "external_id": external_id,
                    "created_at": now,
                },
            )

        # 3) Upsert SUPERADMIN role assignment
        # assigned_by points at the system user (upserted in step 1a above).
        role_assignment_id = _deterministic_role_assignment_id(DEV_ADMIN_USER_ID, SUPERADMIN_ROLE)
        await conn.execute(
            text(
                "INSERT INTO role_assignments "
                "(id, user_id, role, assigned_by, assigned_at) "
                "VALUES (:id, :user_id, :role, :assigned_by, :assigned_at) "
                "ON CONFLICT ON CONSTRAINT uq_role_assignments_user_role DO NOTHING"
            ),
            {
                "id": role_assignment_id,
                "user_id": DEV_ADMIN_USER_ID,
                "role": SUPERADMIN_ROLE,
                "assigned_by": system_user_id,
                "assigned_at": now,
            },
        )

    await engine.dispose()
    print(
        f"seed_dev_admin: SUPERADMIN seeded as user_id={DEV_ADMIN_USER_ID} "
        f"with local identities {external_ids}"
    )


if __name__ == "__main__":
    asyncio.run(seed())
