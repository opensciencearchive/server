from __future__ import annotations

from abc import abstractmethod
from typing import List, Protocol

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class DepositionRepository(Port, Protocol):
    @abstractmethod
    async def get(self, srn: DepositionSRN) -> Deposition | None: ...

    @abstractmethod
    async def save(self, deposition: Deposition) -> None: ...

    @abstractmethod
    async def list(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> List[Deposition]: ...

    @abstractmethod
    async def list_by_owner(
        self,
        owner_id: UserId,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> List[Deposition]: ...

    @abstractmethod
    async def count(self) -> int: ...

    @abstractmethod
    async def count_by_owner(self, owner_id: UserId) -> int: ...
