"""RecordRepository port - persistence interface for records."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.record.model.aggregate import Record
from osa.domain.shared.model.srn import RecordSRN
from osa.domain.shared.port import Port


class RecordRepository(Port, Protocol):
    @abstractmethod
    async def save(self, record: Record) -> None: ...

    @abstractmethod
    async def get(self, srn: RecordSRN) -> Record | None: ...

    @abstractmethod
    async def find_by_source(self, source_type: str, source_id: str) -> Record | None: ...

    @abstractmethod
    async def count(self) -> int: ...
