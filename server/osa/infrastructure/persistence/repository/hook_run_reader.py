"""Postgres adapter for HookRunReader (feature #145 provenance lookup).

A single indexed join over ``hook_runs ⋈ hook_releases`` per batch/deposition —
not per feature row. Reading the validation-owned hook tables here is an
infra-layer concern (the feature store already reads ``records``); the feature
domain depends only on its ``HookRunReader`` port.
"""

from __future__ import annotations

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.feature.port.hook_run_reader import HookRunReader
from osa.infrastructure.persistence.tables import hook_releases_table, hook_runs_table


class PostgresHookRunReader(HookRunReader):
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def run_ids_for_batch(
        self, ingest_run_id: str, batch_index: int
    ) -> dict[str, str]:
        stmt = (
            select(hook_releases_table.c.hook_name, hook_runs_table.c.id)
            .select_from(
                hook_runs_table.join(
                    hook_releases_table,
                    hook_runs_table.c.release_id == hook_releases_table.c.id,
                )
            )
            .where(
                hook_runs_table.c.ingest_run_id == ingest_run_id,
                hook_runs_table.c.batch_index == batch_index,
            )
            .order_by(hook_runs_table.c.started_at)
        )
        return await self._as_map(stmt)

    async def run_ids_for_deposition(self, deposition_id: str) -> dict[str, str]:
        stmt = (
            select(hook_releases_table.c.hook_name, hook_runs_table.c.id)
            .select_from(
                hook_runs_table.join(
                    hook_releases_table,
                    hook_runs_table.c.release_id == hook_releases_table.c.id,
                )
            )
            .where(hook_runs_table.c.deposition_id == deposition_id)
            .order_by(hook_runs_table.c.started_at)
        )
        return await self._as_map(stmt)

    async def _as_map(self, stmt: Select) -> dict[str, str]:
        result = await self.session.execute(stmt)
        # ORDER BY started_at ascending → later (newer) runs overwrite earlier
        # ones, so a re-validated hook resolves to its latest run.
        return {row.hook_name: str(row.id) for row in result.all()}
