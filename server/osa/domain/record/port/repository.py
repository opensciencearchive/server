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
    async def save_many(self, records: list[Record]) -> list[Record]:
        """Multi-row INSERT with ON CONFLICT DO NOTHING. Returns inserted records."""
        ...

    @abstractmethod
    async def get(self, srn: RecordSRN) -> Record | None: ...

    @abstractmethod
    async def count(self) -> int: ...
