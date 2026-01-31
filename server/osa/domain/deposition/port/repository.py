from abc import abstractmethod
from typing import Protocol

from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class DepositionRepository(Port, Protocol):
    @abstractmethod
    async def get(self, srn: DepositionSRN) -> Deposition | None: ...

    @abstractmethod
    async def save(self, deposition: Deposition) -> None: ...
