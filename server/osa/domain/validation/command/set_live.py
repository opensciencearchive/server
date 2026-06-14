"""SetLive — repoint a hook's live pointer to a prior release (#145, US4).

``PUT /hooks/{name}/live`` rolls the live pointer back (or forward) to any
existing release of the hook. Releases are never deleted; history is preserved
and the provenance of rows already produced by other releases is unchanged.
"""

from __future__ import annotations

from osa.domain.auth.model.principal import Principal
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import InvalidStateError
from osa.domain.shared.model.hook import HookName
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.service.hook_registry import HookRegistryService


class SetLive(Command):
    """Repoint the hook's live pointer to ``version`` (an existing release)."""

    name: HookName
    version: int


class LiveSet(Result):
    hook_name: HookName
    live_version: int
    live_release_id: HookReleaseId


class SetLiveHandler(CommandHandler[SetLive, LiveSet]):
    # Authorized by the ``hooks:write`` M2M scope OR an ADMIN role (#145, US5).
    __auth__ = requires_scope("hooks:write")
    principal: Principal
    service: HookRegistryService

    async def run(self, cmd: SetLive) -> LiveSet:
        hook = await self.service.set_live(cmd.name, cmd.version)
        # set_live always points at the target release (it raises NotFoundError
        # otherwise), so live_release_id is non-None here.
        if hook.live_release_id is None:
            raise InvalidStateError(f"Live pointer not set after set_live for {cmd.name}")
        return LiveSet(
            hook_name=hook.name,
            live_version=cmd.version,
            live_release_id=hook.live_release_id,
        )
