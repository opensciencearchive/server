from abc import abstractmethod
from typing import Any, Protocol

from pydantic import BaseModel

from osa.domain.shared.port import Port


class IngestionResult(BaseModel):
    metadata: dict[str, Any]
    filename: str
    # Note: stream is not Pydantic-friendly for serialization,
    # but this DTO is transient within the process.
    stream: Any  # BinaryIO


class IngestionPort(Port, Protocol):
    @abstractmethod
    def ingest(self, url: str) -> IngestionResult:
        """
        Resolves the URL and returns a file stream and extracted metadata.
        """
        ...
