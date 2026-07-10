"""Unit tests for :class:`SessionUnitOfWork`.

The adapter is a thin wrapper: ``commit()`` must delegate to the injected
:class:`AsyncSession`. Driven against an ``AsyncMock`` session so no real DB is
required.
"""

from unittest.mock import AsyncMock

from sqlalchemy.ext.asyncio import AsyncSession

from osa.infrastructure.persistence.unit_of_work import SessionUnitOfWork


async def test_commit_delegates_to_session() -> None:
    session = AsyncMock(spec=AsyncSession)
    uow = SessionUnitOfWork(session)

    await uow.commit()

    session.commit.assert_awaited_once_with()
