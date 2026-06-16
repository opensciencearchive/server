"""GetHookRun — inspect a single hook-run provenance record (#147).

``GET /hooks/runs/{run_id}`` returns the append-only run record: which release
ran, its status/timing/oom_retries, and whether captured logs are available.
Addressed by the globally-unique ``HookRunId``, so it covers both the deposition
and ingestion hook paths (both record ``hook_run`` rows) with no branching.

The raw ``log_ref`` locator (an internal FS path / S3 key) is **never** exposed;
clients see only ``has_logs`` and fetch the bytes via the ``/logs`` sub-resource.
"""

from __future__ import annotations

from datetime import datetime

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.query import Query, QueryHandler, Result
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.model.hook_run import HookRunId, HookRunStatus
from osa.domain.validation.service.hook_registry import HookRegistryService


class GetHookRun(Query):
    run_id: HookRunId


class HookRunDetail(Result):
    id: HookRunId
    release_id: HookReleaseId
    status: HookRunStatus
    started_at: datetime
    finished_at: datetime
    duration_s: float
    oom_retries: int
    has_logs: bool


class GetHookRunHandler(QueryHandler[GetHookRun, HookRunDetail]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    service: HookRegistryService

    async def run(self, cmd: GetHookRun) -> HookRunDetail:
        run = await self.service.get_run(cmd.run_id)
        if run is None:
            raise NotFoundError(f"Hook run not found: {cmd.run_id}")
        return HookRunDetail(
            id=run.id,
            release_id=run.release_id,
            status=run.status,
            started_at=run.started_at,
            finished_at=run.finished_at,
            duration_s=run.duration_s,
            oom_retries=run.oom_retries,
            has_logs=run.log_ref is not None,
        )
