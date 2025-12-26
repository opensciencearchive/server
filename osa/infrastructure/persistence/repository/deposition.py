from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.shared.model.srn import DepositionSRN
from osa.infrastructure.persistence.mappers.deposition import (
    row_to_deposition,
    deposition_to_dict,
)
from osa.infrastructure.persistence.tables import depositions_table


class PostgresDepositionRepository(DepositionRepository):
    """PostgreSQL implementation of DepositionRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, deposition: Deposition) -> None:
        dep_dict = deposition_to_dict(deposition)
        
        existing = await self.get(deposition.srn)
        
        if existing:
            stmt = (
                update(depositions_table)
                .where(depositions_table.c.srn == str(deposition.srn))
                .values(**dep_dict)
            )
        else:
            stmt = insert(depositions_table).values(**dep_dict)
            
        await self.session.execute(stmt)
        await self.session.flush()

    async def get(self, srn: DepositionSRN) -> Deposition | None:
        stmt = select(depositions_table).where(depositions_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_deposition(dict(row)) if row else None
