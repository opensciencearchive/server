from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.port import Port
from osa.domain.shared.model.srn import DepositionSRN


class StoragePort(Port, Protocol):
    @abstractmethod
    def delete_files_for_deposition(self, deposition_id: DepositionSRN) -> None:
        """
        Physically removes all files associated with the deposition from storage.
        """
        ...
