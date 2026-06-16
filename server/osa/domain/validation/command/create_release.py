"""CreateRelease — register a new release for an existing hook (#145, US3).

``POST /hooks/{name}/releases`` mints vN+1 (immutable) and advances the live
pointer; the convention that references the hook is untouched. Idempotent on
``(name, digest)``: re-posting an existing digest is a no-op that returns the
existing release without minting a version or moving the pointer (FR-006/R5).
"""

from __future__ import annotations

from datetime import datetime

from pydantic import Field

from osa.domain.auth.model.principal import Principal
from osa.domain.shared.authorization.gate import requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.model.hook import HookName, OciConfig, OciLimits
from osa.domain.validation.model.hook_release import HookReleaseId
from osa.domain.validation.service.hook_registry import HookRegistryService


class CreateRelease(Command):
    """Register release vN+1 for an existing hook.

    No ``feature`` — the output contract is fixed at the hook's first release
    (FR-002); a differing one is rejected upstream as a contract conflict.
    """

    name: HookName
    image: str
    digest: str
    config: dict = Field(default_factory=dict)
    limits: OciLimits = Field(default_factory=OciLimits)
    source_ref: str  # REQUIRED — reproducibility anchor (FR-005)

    def to_runtime(self) -> OciConfig:
        return OciConfig(
            image=self.image,
            digest=self.digest,
            config=self.config,
            limits=self.limits,
        )


class ReleaseCreated(Result):
    hook_name: HookName
    version: int
    id: HookReleaseId
    digest: str
    source_ref: str
    built_at: datetime
    live: bool
    # Internal-only: lets the router pick 201 (new version) vs 200 (idempotent
    # no-op). Excluded from the wire response (the contract's ReleaseCreated has
    # no such field).
    created: bool = Field(default=True, exclude=True)


class CreateReleaseHandler(CommandHandler[CreateRelease, ReleaseCreated]):
    # Authorized by the ``hooks:write`` M2M scope OR an ADMIN role (#145, US5).
    __auth__ = requires_scope("hooks:write")
    principal: Principal
    service: HookRegistryService

    async def run(self, cmd: CreateRelease) -> ReleaseCreated:
        built_by = str(self.principal.user_id) if self.principal.user_id else None

        # `created` (201 new version vs 200 idempotent no-op) is decided inside
        # the registry's row lock, so it is correct under concurrent identical
        # submissions — no racy pre-read here.
        outcome = await self.service.create_release(
            cmd.name, cmd.to_runtime(), cmd.source_ref, built_by
        )
        release = outcome.release
        hook = await self.service.get_hook(cmd.name)
        is_live = hook is not None and hook.live_release_id == release.id

        return ReleaseCreated(
            hook_name=release.hook_name,
            version=release.version,
            id=release.id,
            digest=release.runtime.digest,
            source_ref=release.source_ref,
            built_at=release.built_at,
            live=is_live,
            created=outcome.created,
        )
