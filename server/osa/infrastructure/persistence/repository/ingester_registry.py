"""Postgres adapter for the ingester registry (#180).

Concurrency-critical operations — version assignment and live-pointer advance —
run under a ``SELECT ... FOR UPDATE`` row lock on the ``ingesters`` row, so
concurrent release submissions for one ingester produce gap-free monotonic
versions and never lose a pointer update. Identical reasoning, and identical
mechanics, to :class:`~osa.infrastructure.persistence.repository.hook_registry.PostgresHookRegistry`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.ingest.model.ingester import Ingester
from osa.domain.ingest.model.ingester_release import (
    IngesterRelease,
    IngesterReleaseId,
    IngesterReleaseOutcome,
)
from osa.domain.ingest.port.ingester_registry import IngesterRegistry
from osa.domain.shared.error import ConflictError, InfrastructureError, NotFoundError
from osa.domain.shared.model.hook import OciConfig, OciLimits
from osa.domain.shared.model.source import IngesterName
from osa.domain.shared.model.srn import LocalId
from osa.infrastructure.persistence.tables import (
    ingester_releases_table,
    ingesters_table,
)


class PostgresIngesterRegistry(IngesterRegistry):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def _to_ingester(row: dict[str, Any]) -> Ingester:
        live_release_id = row["live_release_id"]
        return Ingester(
            name=IngesterName(row["name"]),
            schema_id=LocalId(row["schema_id"]),
            live_release_id=(
                IngesterReleaseId(live_release_id) if live_release_id is not None else None
            ),
            created_at=row["created_at"],
        )

    @staticmethod
    def _to_release(row: dict[str, Any]) -> IngesterRelease:
        return IngesterRelease(
            id=IngesterReleaseId(row["id"]),
            ingester_name=IngesterName(row["ingester_name"]),
            version=row["version"],
            runtime=OciConfig(
                image=row["image"],
                digest=row["digest"],
                config=row["config"] or {},
                limits=OciLimits.model_validate(row["limits"]),
            ),
            source_ref=row["source_ref"],
            built_by=row["built_by"],
            built_at=row["built_at"],
        )

    async def _get_ingester_row(self, name: IngesterName) -> dict[str, Any] | None:
        result = await self.session.execute(
            select(ingesters_table).where(ingesters_table.c.name == name.root)
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None

    async def upsert_identity(self, name: IngesterName, schema_id: LocalId) -> Ingester:
        # Race-safe create-if-absent: two concurrent identical deploys must not
        # collide on the primary key. ON CONFLICT DO NOTHING serializes on PG's
        # internal lock — blocking on a concurrent uncommitted insert until it
        # resolves — so the loser no-ops instead of erroring. The winning row is
        # then read back and its schema binding enforced.
        await self.session.execute(
            pg_insert(ingesters_table)
            .values(
                name=name.root,
                schema_id=schema_id.root,
                live_release_id=None,
                created_at=datetime.now(UTC),
            )
            .on_conflict_do_nothing(index_elements=["name"])
        )
        await self.session.flush()

        row = await self._get_ingester_row(name)
        if row is None:  # pragma: no cover — the upsert above guarantees the row
            raise InfrastructureError(f"ingester {name.root!r} missing immediately after upsert")
        if row["schema_id"] != schema_id.root:
            raise ConflictError(
                f"Ingester {name.root!r} already produces records for schema "
                f"{row['schema_id']!r}; repointing it to {schema_id.root!r} would orphan "
                "the records it has already published",
                code="ingester_schema_conflict",
            )
        return self._to_ingester(row)

    async def create_release(
        self,
        name: IngesterName,
        runtime: OciConfig,
        source_ref: str,
        built_by: str | None,
    ) -> IngesterReleaseOutcome:
        # Row-lock the ingester so concurrent releases serialize. Also asserts
        # the ingester exists.
        locked = await self.session.execute(
            select(ingesters_table).where(ingesters_table.c.name == name.root).with_for_update()
        )
        if locked.mappings().first() is None:
            raise NotFoundError(f"Ingester not found: {name.root}")

        # Idempotency on (ingester_name, digest): return the existing release, no
        # new version, pointer unchanged. Decided under the row lock, so `created`
        # is race-free under concurrent identical submissions.
        duplicate = await self.session.execute(
            select(ingester_releases_table).where(
                and_(
                    ingester_releases_table.c.ingester_name == name.root,
                    ingester_releases_table.c.digest == runtime.digest,
                )
            )
        )
        duplicate_row = duplicate.mappings().first()
        if duplicate_row is not None:
            return IngesterReleaseOutcome(
                release=self._to_release(dict(duplicate_row)), created=False
            )

        highest_version = await self.session.scalar(
            select(func.coalesce(func.max(ingester_releases_table.c.version), 0)).where(
                ingester_releases_table.c.ingester_name == name.root
            )
        )
        next_version = int(highest_version or 0) + 1
        release_id = uuid4()

        await self.session.execute(
            insert(ingester_releases_table).values(
                id=release_id,
                ingester_name=name.root,
                version=next_version,
                image=runtime.image,
                digest=runtime.digest,
                config=runtime.config,
                limits=runtime.limits.model_dump(),
                source_ref=source_ref,
                built_by=built_by,
                built_at=datetime.now(UTC),
            )
        )
        await self.session.execute(
            update(ingesters_table)
            .where(ingesters_table.c.name == name.root)
            .values(live_release_id=release_id)
        )
        await self.session.flush()

        minted = await self.get_release(name, next_version)
        if minted is None:  # pragma: no cover — just inserted in this transaction
            raise InfrastructureError(
                f"ingester release {name.root}@v{next_version} missing immediately after insert"
            )
        return IngesterReleaseOutcome(release=minted, created=True)

    async def set_live(self, name: IngesterName, version: int) -> Ingester:
        locked = await self.session.execute(
            select(ingesters_table).where(ingesters_table.c.name == name.root).with_for_update()
        )
        if locked.mappings().first() is None:
            raise NotFoundError(f"Ingester not found: {name.root}")

        target = await self.get_release(name, version)
        if target is None:
            raise NotFoundError(f"Release not found: {name.root}@v{version}")

        await self.session.execute(
            update(ingesters_table)
            .where(ingesters_table.c.name == name.root)
            .values(live_release_id=target.id)
        )
        await self.session.flush()

        row = await self._get_ingester_row(name)
        if row is None:  # pragma: no cover — locked above
            raise InfrastructureError(f"ingester {name.root!r} vanished during set_live")
        return self._to_ingester(row)

    async def get_ingester(self, name: IngesterName) -> Ingester | None:
        row = await self._get_ingester_row(name)
        return self._to_ingester(row) if row is not None else None

    async def list_ingesters(self) -> list[Ingester]:
        result = await self.session.execute(
            select(ingesters_table).order_by(ingesters_table.c.name)
        )
        return [self._to_ingester(dict(row)) for row in result.mappings().all()]

    async def list_releases(self, name: IngesterName) -> list[IngesterRelease]:
        result = await self.session.execute(
            select(ingester_releases_table)
            .where(ingester_releases_table.c.ingester_name == name.root)
            .order_by(ingester_releases_table.c.version.desc())
        )
        return [self._to_release(dict(row)) for row in result.mappings().all()]

    async def get_release(self, name: IngesterName, version: int) -> IngesterRelease | None:
        result = await self.session.execute(
            select(ingester_releases_table).where(
                and_(
                    ingester_releases_table.c.ingester_name == name.root,
                    ingester_releases_table.c.version == version,
                )
            )
        )
        row = result.mappings().first()
        return self._to_release(dict(row)) if row is not None else None

    async def get_release_by_id(self, release_id: IngesterReleaseId) -> IngesterRelease | None:
        result = await self.session.execute(
            select(ingester_releases_table).where(ingester_releases_table.c.id == release_id)
        )
        row = result.mappings().first()
        return self._to_release(dict(row)) if row is not None else None

    async def resolve_live(self, name: IngesterName) -> IngesterRelease | None:
        result = await self.session.execute(
            select(ingester_releases_table)
            .join(
                ingesters_table,
                ingesters_table.c.live_release_id == ingester_releases_table.c.id,
            )
            .where(ingesters_table.c.name == name.root)
        )
        row = result.mappings().first()
        return self._to_release(dict(row)) if row is not None else None
