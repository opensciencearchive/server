"""Port for the ingester registry (#180).

Persists ingester identities, their immutable versioned releases, and the live
pointer. The adapter owns the concurrency-critical bits — gap-free monotonic
version assignment and live-pointer advance under a row lock on the
``ingesters`` row — exactly as
:class:`~osa.domain.validation.port.hook_registry.HookRegistry` does.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from osa.domain.ingest.model.ingester import Ingester
from osa.domain.ingest.model.ingester_release import (
    IngesterRelease,
    IngesterReleaseId,
    IngesterReleaseOutcome,
)
from osa.domain.shared.model.hook import OciConfig
from osa.domain.shared.model.source import IngesterName
from osa.domain.shared.model.srn import LocalId
from osa.domain.shared.port import Port


class IngesterRegistry(Port, Protocol):
    @abstractmethod
    async def upsert_identity(self, name: IngesterName, schema_id: LocalId) -> Ingester:
        """Create the ingester identity if absent; return the existing or new one.

        If the ingester exists bound to a **different** schema, raise
        ``ConflictError``: records already published under this name belong to
        the original schema, and silently repointing would orphan them.
        """
        ...

    @abstractmethod
    async def create_release(
        self,
        name: IngesterName,
        runtime: OciConfig,
        source_ref: str,
        built_by: str | None,
    ) -> IngesterReleaseOutcome:
        """Mint the next release for an existing ingester and advance live.

        Idempotent on ``(name, digest)``: re-submitting an existing digest
        returns the existing release without minting a version or moving the
        pointer, with ``created == False``. Version assignment and pointer
        advance happen under a row lock on the ``ingesters`` row so concurrent
        submitters serialize, and ``created`` is decided under that same lock so
        it is race-free.
        """
        ...

    @abstractmethod
    async def set_live(self, name: IngesterName, version: int) -> Ingester:
        """Repoint live to an existing release of the ingester (rollback / pin)."""
        ...

    @abstractmethod
    async def get_ingester(self, name: IngesterName) -> Ingester | None: ...

    @abstractmethod
    async def list_ingesters(self) -> list[Ingester]: ...

    @abstractmethod
    async def list_releases(self, name: IngesterName) -> list[IngesterRelease]:
        """All releases for an ingester, version-descending. Empty if absent."""
        ...

    @abstractmethod
    async def get_release(self, name: IngesterName, version: int) -> IngesterRelease | None: ...

    @abstractmethod
    async def get_release_by_id(self, release_id: IngesterReleaseId) -> IngesterRelease | None:
        """Resolve a release by id — the record → run → release provenance hop."""
        ...

    @abstractmethod
    async def resolve_live(self, name: IngesterName) -> IngesterRelease | None:
        """The ingester's current live release, resolved once at run start.

        Snapshotted onto the run so a redeploy mid-run cannot change which code
        the run is attributed to. ``None`` when the ingester has no release.
        """
        ...
