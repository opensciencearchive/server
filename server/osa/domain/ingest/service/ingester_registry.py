"""IngesterRegistryService — business logic for the ingester registry (#180).

Thin orchestration over the :class:`IngesterRegistry` port, mirroring
:class:`~osa.domain.validation.service.hook_registry.HookRegistryService`. The
concurrency-critical version and pointer mechanics live in the adapter.
"""

from __future__ import annotations

from osa.domain.ingest.model.ingester import Ingester
from osa.domain.ingest.model.ingester_release import (
    IngesterRelease,
    IngesterReleaseId,
    IngesterReleaseOutcome,
)
from osa.domain.ingest.port.ingester_registry import IngesterRegistry
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import OciConfig
from osa.domain.shared.model.source import IngesterName
from osa.domain.shared.model.srn import LocalId
from osa.domain.shared.service import Service


class IngesterRegistryService(Service):
    registry: IngesterRegistry

    async def upsert_identity(self, name: IngesterName, schema_id: LocalId) -> Ingester:
        """Create the ingester identity if absent; reject a differing schema."""
        return await self.registry.upsert_identity(name, schema_id)

    async def create_release(
        self,
        name: IngesterName,
        runtime: OciConfig,
        source_ref: str,
        built_by: str | None = None,
    ) -> IngesterReleaseOutcome:
        """Mint vN+1 for an existing ingester (idempotent on digest); advance live."""
        return await self.registry.create_release(name, runtime, source_ref, built_by)

    async def set_live(self, name: IngesterName, version: int) -> Ingester:
        """Repoint the live pointer to a prior release (rollback / pin)."""
        return await self.registry.set_live(name, version)

    async def get_ingester(self, name: IngesterName) -> Ingester | None:
        return await self.registry.get_ingester(name)

    async def list_ingesters(self) -> list[Ingester]:
        return await self.registry.list_ingesters()

    async def list_releases(self, name: IngesterName) -> list[IngesterRelease]:
        return await self.registry.list_releases(name)

    async def get_release(self, name: IngesterName, version: int) -> IngesterRelease | None:
        return await self.registry.get_release(name, version)

    async def get_release_by_id(self, release_id: IngesterReleaseId) -> IngesterRelease | None:
        return await self.registry.get_release_by_id(release_id)

    async def require_live_release(self, name: IngesterName) -> IngesterRelease:
        """Resolve the live release for a run, or fail with a nameable reason.

        Callers start a run with this: an ingester that exists but has never had
        a release is a deploy-time mistake, and a run attributed to no code at
        all is exactly the provenance hole this registry closes.
        """
        release = await self.registry.resolve_live(name)
        if release is None:
            raise NotFoundError(f"Ingester {name.root!r} has no live release")
        return release
