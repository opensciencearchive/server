"""SQLAlchemy adapter for the :class:`UnitOfWork` port."""

from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.port.unit_of_work import UnitOfWork


class SessionUnitOfWork(UnitOfWork):
    """Commits the request/worker-scoped :class:`AsyncSession`.

    After ``commit`` the session lazily begins a new transaction on its next
    use, so orchestrators can keep writing through the same session. The
    scope's final commit at teardown is a harmless no-op when nothing new was
    written since the last checkpoint.
    """

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def commit(self) -> None:
        await self._session.commit()
