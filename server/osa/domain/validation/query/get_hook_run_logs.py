"""GetHookRunLogs — stream a hook run's captured container logs (#147).

``GET /hooks/runs/{run_id}/logs`` streams the ``hook.log`` artifact captured when
the hook container errored (#145). 404s when the run is unknown or carries no
logs (a passed run, or a failure where log capture itself failed). ADMIN-only:
the logs are tenant stdout/stderr and there is no per-run ownership model.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.query import Query, QueryHandler, Result
from osa.domain.validation.model.hook_run import HookRunId
from osa.domain.validation.port.storage import HookStoragePort
from osa.domain.validation.service.hook_registry import HookRegistryService


class GetHookRunLogs(Query):
    run_id: HookRunId


class HookRunLogStream(Result, arbitrary_types_allowed=True):
    stream: AsyncIterator[bytes]


class GetHookRunLogsHandler(QueryHandler[GetHookRunLogs, HookRunLogStream]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal
    service: HookRegistryService
    hook_storage: HookStoragePort

    async def run(self, cmd: GetHookRunLogs) -> HookRunLogStream:
        run = await self.service.get_run(cmd.run_id)
        if run is None:
            raise NotFoundError(f"Hook run not found: {cmd.run_id}")
        if run.log_ref is None:
            raise NotFoundError(f"No logs captured for hook run: {cmd.run_id}")
        stream = await self.hook_storage.read_hook_log(run.log_ref)
        return HookRunLogStream(stream=stream)
