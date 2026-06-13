"""HookRegistryService — business logic for the hook registry (feature #145).

Thin orchestration over the :class:`HookRegistry` port: upsert identities,
mint releases (advancing the live pointer), repoint live for rollback, and
resolve the live release set once at run start (snapshot, R8). The
concurrency-critical version/pointer mechanics live in the adapter.
"""

from __future__ import annotations

from osa.domain.shared.model.hook import HookName, OciConfig, TableFeatureSpec
from osa.domain.shared.service import Service
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease
from osa.domain.validation.model.hook_run import HookRun
from osa.domain.validation.port.hook_registry import HookRegistry


class HookRegistryService(Service):
    registry: HookRegistry

    async def upsert_identity(self, name: HookName, feature: TableFeatureSpec) -> Hook:
        """Create the hook identity if absent; reject a differing contract."""
        return await self.registry.upsert_identity(name, feature)

    async def create_release(
        self,
        name: HookName,
        runtime: OciConfig,
        source_ref: str,
        built_by: str | None = None,
    ) -> HookRelease:
        """Mint vN+1 for an existing hook (idempotent on digest); advance live."""
        return await self.registry.create_release(name, runtime, source_ref, built_by)

    async def set_live(self, name: HookName, version: int) -> Hook:
        """Repoint the live pointer to a prior release (rollback / pin)."""
        return await self.registry.set_live(name, version)

    async def get_hook(self, name: HookName) -> Hook | None:
        return await self.registry.get_hook(name)

    async def list_hooks(self) -> list[Hook]:
        return await self.registry.list_hooks()

    async def list_releases(self, name: HookName) -> list[HookRelease]:
        return await self.registry.list_releases(name)

    async def get_release(self, name: HookName, version: int) -> HookRelease | None:
        return await self.registry.get_release(name, version)

    async def resolve_live(self, names: list[HookName]) -> dict[HookName, HookRelease]:
        """Resolve the live release for each hook once, for snapshotting (R8)."""
        return await self.registry.resolve_live(names)

    async def record_run(self, run: HookRun) -> None:
        """Persist a completed hook_run (append-only provenance anchor)."""
        await self.registry.record_run(run)
