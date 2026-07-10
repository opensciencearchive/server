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
    async def srns_for_ingest_batch(
        self, ingest_run_id: str, batch_index: int
    ) -> dict[str, RecordSRN]:
        """Map upstream_source → SRN for records published by one ingest batch.

        Recovers a batch's publish mapping on workflow retry: bulk_publish's
        ON CONFLICT returns only newly inserted rows, so a redo needs the
        DB-authoritative answer. Records published by an EARLIER batch carry
        that batch's index and are correctly excluded.
        """
        ...

    @abstractmethod
    async def count(self) -> int: ...
