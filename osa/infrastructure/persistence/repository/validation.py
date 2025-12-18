from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import TraitSRN, ValidationRunSRN
from osa.domain.validation.model import Trait, ValidationRun
from osa.domain.validation.port.repository import (
    TraitRepository,
    ValidationRunRepository,
)
from osa.infrastructure.persistence.mappers.validation import (
    row_to_trait,
    row_to_validation_run,
    trait_to_dict,
    validation_run_to_dict,
)
from osa.infrastructure.persistence.tables import traits_table, validation_runs_table


class PostgresTraitRepository(TraitRepository):
    """PostgreSQL implementation of TraitRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, srn: TraitSRN) -> Trait | None:
        stmt = select(traits_table).where(traits_table.c.srn == str(srn))
        result = await self._session.execute(stmt)
        row = result.mappings().first()
        return row_to_trait(dict(row)) if row else None

    async def get_or_fetch(self, srn: TraitSRN) -> Trait:
        """Get locally or fetch from remote. For now, only local lookup."""
        trait = await self.get(srn)
        if trait is None:
            raise ValueError(f"Trait not found: {srn}")
        return trait

    async def save(self, trait: Trait) -> None:
        trait_dict = trait_to_dict(trait)
        existing = await self.get(trait.srn)

        if existing:
            stmt = (
                update(traits_table)
                .where(traits_table.c.srn == str(trait.srn))
                .values(**trait_dict)
            )
        else:
            stmt = insert(traits_table).values(**trait_dict)

        await self._session.execute(stmt)
        await self._session.flush()

    async def list(self) -> list[Trait]:
        stmt = select(traits_table)
        result = await self._session.execute(stmt)
        rows = result.mappings().all()
        return [row_to_trait(dict(row)) for row in rows]


class PostgresValidationRunRepository(ValidationRunRepository):
    """PostgreSQL implementation of ValidationRunRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, srn: ValidationRunSRN) -> ValidationRun | None:
        stmt = select(validation_runs_table).where(
            validation_runs_table.c.srn == str(srn)
        )
        result = await self._session.execute(stmt)
        row = result.mappings().first()
        return row_to_validation_run(dict(row)) if row else None

    async def save(self, run: ValidationRun) -> None:
        run_dict = validation_run_to_dict(run)
        existing = await self.get(run.srn)

        if existing:
            stmt = (
                update(validation_runs_table)
                .where(validation_runs_table.c.srn == str(run.srn))
                .values(**run_dict)
            )
        else:
            stmt = insert(validation_runs_table).values(**run_dict)

        await self._session.execute(stmt)
        await self._session.flush()

