from abc import abstractmethod
from typing import TYPE_CHECKING, List, Protocol

from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.port import Port

if TYPE_CHECKING:
    from osa.domain.deposition.model.convention import Convention


class ConventionRepository(Port, Protocol):
    @abstractmethod
    async def save(self, convention: "Convention") -> None: ...

    @abstractmethod
    async def get(self, srn: ConventionSRN) -> "Convention | None": ...

    @abstractmethod
    async def list(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> "List[Convention]": ...

    @abstractmethod
    async def exists(self, srn: ConventionSRN) -> bool: ...

    @abstractmethod
    async def list_with_source(self) -> "List[Convention]":
        """Return conventions that have a source defined (SQL-level filter)."""
        ...
