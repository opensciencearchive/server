"""Postgres adapter for the hook registry (feature #145).

Concurrency-critical operations (version assignment, live-pointer advance) run
under a ``SELECT ... FOR UPDATE`` row lock on the ``hooks`` row so concurrent
release submissions to one hook produce gap-free monotonic versions and never
lose a pointer update (research R7).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, func, insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.shared.error import ConflictError, NotFoundError
from osa.domain.shared.model.hook import (
    HookName,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId, ReleaseOutcome
from osa.domain.validation.model.hook_run import HookRun
from osa.domain.validation.port.hook_registry import HookRegistry
from osa.infrastructure.persistence.tables import (
    hook_releases_table,
    hook_runs_table,
    hooks_table,
)


class PostgresHookRegistry(HookRegistry):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def _to_hook(row: dict[str, Any]) -> Hook:
        live = row["live_release_id"]
        return Hook(
            name=row["name"],
            feature=TableFeatureSpec.model_validate(row["feature_spec"]),
            live_release_id=HookReleaseId(live) if live is not None else None,
            created_at=row["created_at"],
        )

    @staticmethod
    def _to_release(row: dict[str, Any]) -> HookRelease:
        return HookRelease(
            id=HookReleaseId(row["id"]),
            hook_name=row["hook_name"],
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

    async def upsert_identity(self, name: HookName, feature: TableFeatureSpec) -> Hook:
        existing = await self._get_hook_row(name)
        if existing is not None:
            current = TableFeatureSpec.model_validate(existing["feature_spec"])
            if current != feature:
                raise ConflictError(
                    f"Hook {name!r} already exists with a different feature contract; "
                    "the output contract is fixed across releases"
                )
            return self._to_hook(existing)

        await self.session.execute(
            insert(hooks_table).values(
                name=name.root,
                feature_spec=feature.model_dump(),
                live_release_id=None,
                created_at=datetime.now(UTC),
            )
        )
        await self.session.flush()
        row = await self._get_hook_row(name)
        assert row is not None
        return self._to_hook(row)

    async def create_release(
        self,
        name: HookName,
        runtime: OciConfig,
        source_ref: str,
        built_by: str | None,
    ) -> ReleaseOutcome:
        # Row-lock the hook so concurrent releases serialize (R7). Also asserts
        # the hook exists.
        locked = await self.session.execute(
            select(hooks_table).where(hooks_table.c.name == name.root).with_for_update()
        )
        hook_row = locked.mappings().first()
        if hook_row is None:
            raise NotFoundError(f"Hook not found: {name}")

        # Idempotency on (hook_name, digest): return the existing release, no
        # new version, pointer unchanged (R5). Decided under the row lock, so
        # `created` is race-free under concurrent identical submissions.
        dup = await self.session.execute(
            select(hook_releases_table).where(
                and_(
                    hook_releases_table.c.hook_name == name.root,
                    hook_releases_table.c.digest == runtime.digest,
                )
            )
        )
        dup_row = dup.mappings().first()
        if dup_row is not None:
            return ReleaseOutcome(release=self._to_release(dict(dup_row)), created=False)

        max_version = await self.session.scalar(
            select(func.coalesce(func.max(hook_releases_table.c.version), 0)).where(
                hook_releases_table.c.hook_name == name.root
            )
        )
        next_version = int(max_version or 0) + 1
        release_id = uuid4()

        await self.session.execute(
            insert(hook_releases_table).values(
                id=release_id,
                hook_name=name.root,
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
        # Advance the live pointer.
        await self.session.execute(
            update(hooks_table)
            .where(hooks_table.c.name == name.root)
            .values(live_release_id=release_id)
        )
        await self.session.flush()

        created = await self.get_release(name, next_version)
        assert created is not None
        return ReleaseOutcome(release=created, created=True)

    async def set_live(self, name: HookName, version: int) -> Hook:
        locked = await self.session.execute(
            select(hooks_table).where(hooks_table.c.name == name.root).with_for_update()
        )
        if locked.mappings().first() is None:
            raise NotFoundError(f"Hook not found: {name}")

        target = await self.get_release(name, version)
        if target is None:
            raise NotFoundError(f"Release not found: {name}@v{version}")

        await self.session.execute(
            update(hooks_table)
            .where(hooks_table.c.name == name.root)
            .values(live_release_id=target.id)
        )
        await self.session.flush()
        row = await self._get_hook_row(name)
        assert row is not None
        return self._to_hook(row)

    async def get_hook(self, name: HookName) -> Hook | None:
        row = await self._get_hook_row(name)
        return self._to_hook(row) if row is not None else None

    async def list_hooks(self) -> list[Hook]:
        result = await self.session.execute(select(hooks_table).order_by(hooks_table.c.name))
        return [self._to_hook(dict(r)) for r in result.mappings().all()]

    async def list_releases(self, name: HookName) -> list[HookRelease]:
        result = await self.session.execute(
            select(hook_releases_table)
            .where(hook_releases_table.c.hook_name == name.root)
            .order_by(hook_releases_table.c.version.desc())
        )
        return [self._to_release(dict(r)) for r in result.mappings().all()]

    async def get_release(self, name: HookName, version: int) -> HookRelease | None:
        result = await self.session.execute(
            select(hook_releases_table).where(
                and_(
                    hook_releases_table.c.hook_name == name.root,
                    hook_releases_table.c.version == version,
                )
            )
        )
        row = result.mappings().first()
        return self._to_release(dict(row)) if row else None

    async def get_release_by_id(self, release_id: object) -> HookRelease | None:
        rid = release_id if isinstance(release_id, UUID) else UUID(str(release_id))
        result = await self.session.execute(
            select(hook_releases_table).where(hook_releases_table.c.id == rid)
        )
        row = result.mappings().first()
        return self._to_release(dict(row)) if row else None

    async def record_run(self, run: HookRun) -> None:
        await self.session.execute(
            insert(hook_runs_table).values(
                id=run.id,
                release_id=run.release_id,
                status=run.status.value,
                started_at=run.started_at,
                finished_at=run.finished_at,
                duration_s=run.duration_s,
                oom_retries=run.oom_retries,
                log_ref=run.log_ref,
            )
        )
        await self.session.flush()

    async def resolve_live(self, names: list[HookName]) -> dict[HookName, HookRelease]:
        if not names:
            return {}
        stmt = (
            select(hook_releases_table)
            .select_from(
                hooks_table.join(
                    hook_releases_table,
                    hooks_table.c.live_release_id == hook_releases_table.c.id,
                )
            )
            .where(hooks_table.c.name.in_([n.root for n in names]))
        )
        result = await self.session.execute(stmt)
        return {
            HookName(row["hook_name"]): self._to_release(dict(row))
            for row in result.mappings().all()
        }

    async def _get_hook_row(self, name: HookName) -> dict[str, Any] | None:
        result = await self.session.execute(
            select(hooks_table).where(hooks_table.c.name == name.root)
        )
        row = result.mappings().first()
        return dict(row) if row else None
