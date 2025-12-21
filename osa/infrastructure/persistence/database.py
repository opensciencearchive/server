"""Database engine and session factory creation."""

import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from osa.config import Config


def _expand_sqlite_path(url: str) -> str:
    """Expand ~ in SQLite URLs and ensure parent directory exists."""
    if not url.startswith("sqlite"):
        return url

    # Extract path from URL (sqlite+aiosqlite:///path or sqlite:///path)
    prefix_end = url.index("///") + 3
    prefix = url[:prefix_end]
    path = url[prefix_end:]

    # Expand ~ and make absolute
    expanded = os.path.expanduser(path)
    abs_path = os.path.abspath(expanded)

    # Ensure parent directory exists
    parent = Path(abs_path).parent
    parent.mkdir(parents=True, exist_ok=True)

    return f"{prefix}{abs_path}"


def create_db_engine(config: Config) -> AsyncEngine:
    """Create async database engine.

    Handles SQLite and PostgreSQL with appropriate settings.
    """
    url = _expand_sqlite_path(config.database.url)
    is_sqlite = url.startswith("sqlite")

    # SQLite-specific settings
    if is_sqlite:
        engine_kwargs: dict[str, Any] = {
            "echo": config.database.echo,
            # Use StaticPool for SQLite to allow same connection across threads
            # This is needed for async SQLite with aiosqlite
            "poolclass": StaticPool,
            "connect_args": {"check_same_thread": False},
        }
    else:
        # PostgreSQL settings
        engine_kwargs = {
            "echo": config.database.echo,
            "pool_pre_ping": True,
            "pool_size": 5,
            "max_overflow": 10,
        }

    return create_async_engine(url, **engine_kwargs)


def create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    """Create session factory for dependency injection."""
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )


@asynccontextmanager
async def get_session(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Get database session with automatic cleanup."""
    async with session_factory() as session:
        try:
            yield session
        finally:
            await session.close()
