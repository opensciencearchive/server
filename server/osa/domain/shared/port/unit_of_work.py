"""UnitOfWork port — a durable checkpoint at a workflow stage boundary."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.port import Port


class UnitOfWork(Port, Protocol):
    """Commits work-in-progress so later failures cannot roll it back.

    Workflow orchestrators call this at stage boundaries: it makes prior
    writes durable and releases the open transaction before parking on
    long-running container awaits.
    """

    @abstractmethod
    async def commit(self) -> None:
        """Commit all work accumulated since the last commit."""
        ...
