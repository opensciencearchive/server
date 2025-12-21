"""Database migration utilities.

Migrations are run synchronously at startup before the async server starts.
This keeps the async/sync boundary clean.
"""

import logging
from pathlib import Path

from alembic import command
from alembic.config import Config as AlembicConfig

logger = logging.getLogger(__name__)

# Project root where alembic.ini lives
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent


def to_sync_url(database_url: str) -> str:
    """Convert async database URL to sync equivalent for migrations.

    Alembic runs synchronously, so we need sync drivers:
    - sqlite+aiosqlite:/// -> sqlite:///
    - postgresql+asyncpg:// -> postgresql://
    """
    url = database_url.replace("+aiosqlite", "").replace("+asyncpg", "")
    # Expand ~ in SQLite paths
    if "sqlite:///" in url:
        parts = url.split("///", 1)
        if len(parts) == 2 and parts[1].startswith("~"):
            expanded = str(Path(parts[1]).expanduser())
            url = f"sqlite:///{expanded}"
    return url


def get_alembic_config(database_url: str) -> AlembicConfig:
    """Create Alembic config with the given database URL."""
    alembic_ini = PROJECT_ROOT / "alembic.ini"
    config = AlembicConfig(str(alembic_ini))
    # Convert to sync URL for migrations
    sync_url = to_sync_url(database_url)
    config.set_main_option("sqlalchemy.url", sync_url)
    return config


def run_migrations(database_url: str) -> None:
    """Run pending Alembic migrations.

    This is synchronous and should be called before the async server starts.
    """
    sync_url = to_sync_url(database_url)

    # For SQLite, ensure the directory exists
    if "sqlite:///" in sync_url:
        db_path = sync_url.split("///")[-1]
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    config = get_alembic_config(database_url)
    command.upgrade(config, "head")
    logger.info("Database migrations complete")
