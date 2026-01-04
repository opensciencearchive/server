from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.model.srn import ValidationRunSRN
from osa.domain.validation.model import ValidationRun
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.infrastructure.persistence.mappers.validation import (
    row_to_validation_run,
    validation_run_to_dict,
)
from osa.infrastructure.persistence.tables import validation_runs_table


class PostgresValidationRunRepository(ValidationRunRepository):
    """PostgreSQL implementation of ValidationRunRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get(self, srn: ValidationRunSRN) -> ValidationRun | None:
        stmt = select(validation_runs_table).where(validation_runs_table.c.srn == str(srn))
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
