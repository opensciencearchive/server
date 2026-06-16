from typing import Any, List

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import FileRequirements
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSlug, LocalId, SchemaId, Semver
from osa.infrastructure.persistence.tables import conventions_table


def _convention_to_row(convention: Convention) -> dict[str, Any]:
    return {
        "id": convention.id.root,
        "title": convention.title,
        "description": convention.description,
        "schema_id": convention.schema_id.id.root,
        "schema_version": convention.schema_id.version.root,
        "file_requirements": convention.file_requirements.model_dump(),
        "hooks": [name.root for name in convention.hooks],  # hook-name registry refs
        "source": convention.ingester.model_dump() if convention.ingester else None,
        "created_at": convention.created_at,
    }


def _row_to_convention(row: dict[str, Any]) -> Convention:
    source_data = row.get("source")
    return Convention(
        id=ConventionSlug.parse(row["id"]),
        title=row["title"],
        description=row["description"],
        schema_id=SchemaId(
            id=LocalId(row["schema_id"]),
            version=Semver.from_string(row["schema_version"]),
        ),
        file_requirements=FileRequirements.model_validate(row["file_requirements"]),
        hooks=list(row.get("hooks") or []),
        ingester=IngesterDefinition.model_validate(source_data) if source_data else None,
        created_at=row["created_at"],
    )


class PostgresConventionRepository(ConventionRepository):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def save(self, convention: Convention) -> None:
        # Conventions are mutable, slug-keyed (design-revisions §3): deploy is a
        # declarative upsert — re-declaring updates the convention in place,
        # preserving its original created_at.
        row = _convention_to_row(convention)
        stmt = pg_insert(conventions_table).values(**row)
        stmt = stmt.on_conflict_do_update(
            index_elements=[conventions_table.c.id],
            set_={
                "title": stmt.excluded.title,
                "description": stmt.excluded.description,
                "schema_id": stmt.excluded.schema_id,
                "schema_version": stmt.excluded.schema_version,
                "file_requirements": stmt.excluded.file_requirements,
                "hooks": stmt.excluded.hooks,
                "source": stmt.excluded.source,
            },
        )
        await self.session.execute(stmt)
        await self.session.flush()

    async def get(self, id: ConventionSlug) -> Convention | None:
        stmt = select(conventions_table).where(conventions_table.c.id == id.root)
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

    async def exists(self, id: ConventionSlug) -> bool:
        stmt = select(conventions_table.c.id).where(conventions_table.c.id == id.root)
        result = await self.session.execute(stmt)
        return result.first() is not None

    async def list_with_source(self) -> List[Convention]:
        stmt = (
            select(conventions_table)
            .where(conventions_table.c.source.isnot(None))
            .order_by(conventions_table.c.created_at.desc())
        )
        result = await self.session.execute(stmt)
        return [_row_to_convention(dict(r)) for r in result.mappings().all()]
