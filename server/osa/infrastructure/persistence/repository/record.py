"""PostgreSQL implementation of RecordRepository."""

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
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

    async def save_many(self, records: list[Record]) -> list[Record]:
        """Multi-row INSERT with ON CONFLICT DO NOTHING.

        Returns the records that were actually inserted (duplicates are skipped).
        """
        if not records:
            return []
        values = [record_to_dict(r) for r in records]
        stmt = (
            insert(records_table)
            .values(values)
            .on_conflict_do_nothing(
                index_elements=[
                    text("(source->>'type')"),
                    text("(source->>'id')"),
                ],
            )
            .returning(records_table.c.srn)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        inserted_srns = {row[0] for row in result.fetchall()}
        return [r for r in records if str(r.srn) in inserted_srns]

    async def get(self, srn: RecordSRN) -> Record | None:
        """Get a record by SRN."""
        stmt = select(records_table).where(records_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_record(dict(row)) if row else None

    async def count(self) -> int:
        """Count total records in the database."""
        stmt = select(func.count()).select_from(records_table)
        result = await self.session.execute(stmt)
        return result.scalar() or 0
