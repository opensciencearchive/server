"""PostgreSQL implementation of RecordRepository."""

from sqlalchemy import Integer, func, select, text
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

    async def srns_for_ingest_batch(
        self, ingest_run_id: str, batch_index: int
    ) -> dict[str, RecordSRN]:
        """Map upstream_source → SRN for records published by one ingest batch.

        Recovers a batch's publish mapping on workflow retry: bulk_publish's
        ON CONFLICT returns only newly inserted rows, so a redo needs the
        DB-authoritative answer. Records published by an EARLIER batch carry
        that batch's index and are correctly excluded.
        """
        source = records_table.c.source
        stmt = select(
            records_table.c.srn,
            source["upstream_source"].astext,
        ).where(
            source["type"].astext == "ingest",
            source["ingest_run_id"].astext == ingest_run_id,
            source["batch_index"].astext.cast(Integer) == batch_index,
        )
        result = await self.session.execute(stmt)
        return {upstream_source: RecordSRN.parse(srn) for srn, upstream_source in result.fetchall()}

    async def count(self) -> int:
        """Count total records in the database."""
        stmt = select(func.count()).select_from(records_table)
        result = await self.session.execute(stmt)
        return result.scalar() or 0
