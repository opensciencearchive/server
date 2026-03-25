"""PostgreSQL implementation of RecordRepository."""

from sqlalchemy import func, insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.record.model.aggregate import Record
from osa.domain.record.port.repository import RecordRepository
from osa.domain.shared.model.srn import RecordSRN
from osa.infrastructure.persistence.mappers.record import record_to_dict, row_to_record
from osa.infrastructure.persistence.tables import records_table


class PostgresRecordRepository(RecordRepository):
    """PostgreSQL implementation of RecordRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, record: Record) -> None:
        """Persist a record. Records are immutable, so this is insert-only."""
        record_dict = record_to_dict(record)
        stmt = insert(records_table).values(**record_dict)
        await self.session.execute(stmt)
        await self.session.flush()

    async def get(self, srn: RecordSRN) -> Record | None:
        """Get a record by SRN."""
        stmt = select(records_table).where(records_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_record(dict(row)) if row else None

    async def find_by_source(self, source_type: str, source_id: str) -> Record | None:
        """Find a record by source type and id."""
        stmt = select(records_table).where(
            records_table.c.source["type"].as_string() == source_type,
            records_table.c.source["id"].as_string() == source_id,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_record(dict(row)) if row else None

    async def count(self) -> int:
        """Count total records in the database."""
        stmt = select(func.count()).select_from(records_table)
        result = await self.session.execute(stmt)
        return result.scalar() or 0
