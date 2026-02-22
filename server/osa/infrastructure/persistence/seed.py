"""Database seed data for required system rows."""

import logging
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from osa.domain.auth.model.value import SYSTEM_USER_ID

logger = logging.getLogger(__name__)


async def ensure_system_user(engine: AsyncEngine) -> None:
    """Ensure the system user row exists. Idempotent."""
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO users (id, display_name, created_at) "
                "VALUES (:id, :name, :created_at) "
                "ON CONFLICT (id) DO NOTHING"
            ),
            {
                "id": str(SYSTEM_USER_ID),
                "name": "System",
                "created_at": datetime.now(UTC),
            },
        )
    logger.info("System user seeded (id=%s)", SYSTEM_USER_ID)
