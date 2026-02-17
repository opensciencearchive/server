from __future__ import annotations

from typing import List

from sqlalchemy import func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.auth.model.identity import Identity
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.shared.authorization.decorators import reads, writes
from osa.domain.shared.authorization.resource import has_role, owner
from osa.domain.shared.model.srn import DepositionSRN
from osa.infrastructure.persistence.mappers.deposition import (
    deposition_to_dict,
    row_to_deposition,
)
from osa.infrastructure.persistence.tables import depositions_table


class PostgresDepositionRepository(DepositionRepository):
    """PostgreSQL implementation of DepositionRepository."""

    def __init__(self, session: AsyncSession, identity: Identity) -> None:
        self.session = session
        self._identity = identity

    @reads(owner() | has_role(Role.CURATOR))
    async def get(self, srn: DepositionSRN) -> Deposition | None:
        stmt = select(depositions_table).where(depositions_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return row_to_deposition(dict(row)) if row else None

    @writes(owner())
    async def save(self, deposition: Deposition) -> None:
        dep_dict = deposition_to_dict(deposition)

        # Check if exists (bypass decorator for internal lookup)
        stmt = select(depositions_table).where(depositions_table.c.srn == str(deposition.srn))
        result = await self.session.execute(stmt)
        existing = result.mappings().first()

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

    async def list(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> List[Deposition]:
        stmt = select(depositions_table).order_by(depositions_table.c.created_at.desc())
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)
        return [row_to_deposition(dict(r)) for r in result.mappings().all()]

    async def count(self) -> int:
        stmt = select(func.count()).select_from(depositions_table)
        result = await self.session.execute(stmt)
        return result.scalar_one()

    async def count_by_owner(self, owner_id: UserId) -> int:
        stmt = (
            select(func.count())
            .select_from(depositions_table)
            .where(depositions_table.c.owner_id == str(owner_id))
        )
        result = await self.session.execute(stmt)
        return result.scalar_one()

    async def list_by_owner(
        self,
        owner_id: UserId,
        *,
        limit: int | None = None,
        offset: int | None = None,
    ) -> List[Deposition]:
        stmt = (
            select(depositions_table)
            .where(depositions_table.c.owner_id == str(owner_id))
            .order_by(depositions_table.c.created_at.desc())
        )
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)
        return [row_to_deposition(dict(r)) for r in result.mappings().all()]
