"""Port for the hook registry (feature #145).

Persists hook identities, their immutable versioned releases, and the live
pointer. The adapter is responsible for the concurrency-critical bits:
gap-free monotonic version assignment and live-pointer advance under a row
lock on the ``hooks`` row (research R7).
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.model.hook import HookName, OciConfig, TableFeatureSpec
from osa.domain.shared.port import Port
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease
from osa.domain.validation.model.hook_run import HookRun


class HookRegistry(Port, Protocol):
    @abstractmethod
    async def upsert_identity(self, name: HookName, feature: TableFeatureSpec) -> Hook:
        """Create the hook identity if absent; return the (existing or new) hook.

        If the hook exists with a **different** ``feature`` contract, raise
        ``ConflictError`` (the contract is fixed across releases, FR-002/FR-016).
        """
        ...

    @abstractmethod
    async def create_release(
        self,
        name: HookName,
        runtime: OciConfig,
        source_ref: str,
        built_by: str | None,
    ) -> HookRelease:
        """Mint the next release for an existing hook and advance the live pointer.

        Idempotent on ``(name, digest)``: re-submitting an existing digest returns
        the existing release without minting a new version or moving the pointer
        (FR-006/R5). Version assignment + pointer advance happen under a row lock
        on the ``hooks`` row so concurrent submitters serialize (FR-009/R7).
        """
        ...

    @abstractmethod
    async def set_live(self, name: HookName, version: int) -> Hook:
        """Repoint the live pointer to an existing release of the hook (rollback)."""
        ...

    @abstractmethod
    async def get_hook(self, name: HookName) -> Hook | None: ...

    @abstractmethod
    async def list_hooks(self) -> list[Hook]: ...

    @abstractmethod
    async def list_releases(self, name: HookName) -> list[HookRelease]:
        """All releases for a hook, version-descending. Empty if hook absent."""
        ...

    @abstractmethod
    async def get_release(self, name: HookName, version: int) -> HookRelease | None: ...

    @abstractmethod
    async def get_release_by_id(self, release_id: object) -> HookRelease | None: ...

    @abstractmethod
    async def record_run(self, run: HookRun) -> None:
        """Persist a completed hook_run row (append-only provenance anchor)."""
        ...

    @abstractmethod
    async def run_ids_for_batch(
        self, ingest_run_id: str, batch_index: int
    ) -> dict[HookName, str]:
        """``{hook_name: hook_run_id}`` for one ingest batch (latest per hook).

        Reconstructs feature provenance at insert time from the keys the
        consumer already holds, instead of threading run ids through events.
        """
        ...

    @abstractmethod
    async def run_ids_for_deposition(self, deposition_id: str) -> dict[HookName, str]:
        """``{hook_name: hook_run_id}`` for a deposition (latest per hook)."""
        ...

    @abstractmethod
    async def resolve_live(self, names: list[HookName]) -> dict[HookName, HookRelease]:
        """Resolve each hook's current live release in one indexed lookup.

        Called once at run start and snapshotted for the run's duration (R8).
        Hooks with no live release are omitted from the result.
        """
        ...
