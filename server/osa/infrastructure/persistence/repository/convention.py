from typing import Any, List

from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import ConventionSRN, SchemaSRN
from osa.infrastructure.persistence.tables import conventions_table


def _convention_to_row(convention: Convention) -> dict[str, Any]:
    return {
        "srn": str(convention.srn),
        "title": convention.title,
        "description": convention.description,
        "schema_srn": str(convention.schema_srn),
        "file_requirements": convention.file_requirements.model_dump(),
        "hooks": [h.model_dump() for h in convention.hooks],
        "created_at": convention.created_at,
    }


def _row_to_convention(row: dict[str, Any]) -> Convention:
    return Convention(
        srn=ConventionSRN.parse(row["srn"]),
        title=row["title"],
        description=row.get("description"),
        schema_srn=SchemaSRN.parse(row["schema_srn"]),
        file_requirements=FileRequirements.model_validate(row["file_requirements"]),
        hooks=[HookDefinition.model_validate(h) for h in (row.get("hooks") or [])],
        created_at=row["created_at"],
    )


class PostgresConventionRepository(ConventionRepository):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, convention: Convention) -> None:
        row = _convention_to_row(convention)
        await self.session.execute(insert(conventions_table).values(**row))
        await self.session.flush()

    async def get(self, srn: ConventionSRN) -> Convention | None:
        stmt = select(conventions_table).where(conventions_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_convention(dict(row)) if row else None

    async def list(
        self, *, limit: int | None = None, offset: int | None = None
    ) -> List[Convention]:
        stmt = select(conventions_table).order_by(conventions_table.c.created_at.desc())
        if offset is not None:
            stmt = stmt.offset(offset)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self.session.execute(stmt)
        return [_row_to_convention(dict(r)) for r in result.mappings().all()]

    async def exists(self, srn: ConventionSRN) -> bool:
        stmt = select(conventions_table.c.srn).where(conventions_table.c.srn == str(srn))
        result = await self.session.execute(stmt)
        return result.first() is not None
