from abc import abstractmethod
from collections.abc import AsyncIterator
from typing import Protocol

from osa.domain.deposition.model.value import DepositionFile
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.port import Port


class FileStoragePort(Port, Protocol):
    @abstractmethod
    async def save_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
        content: bytes,
        size: int,
    ) -> DepositionFile: ...

    @abstractmethod
    async def get_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> AsyncIterator[bytes]: ...

    @abstractmethod
    async def delete_file(
        self,
        deposition_id: DepositionSRN,
        filename: str,
    ) -> None: ...

    @abstractmethod
    async def delete_files_for_deposition(
        self,
        deposition_id: DepositionSRN,
    ) -> None: ...
