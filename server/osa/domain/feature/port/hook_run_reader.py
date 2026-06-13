"""Port for resolving the hook_run that produced a batch/deposition's features.

Feature insertion stamps every feature row with the ``run_id`` that produced it
(feature #145 provenance). The run was recorded at execution time (keyed by the
ingest batch or the deposition); this read port reconstructs the
``{hook_name: hook_run_id}`` map at insert time from those keys, so the ids
don't have to ride through the (curation/record) event chain as pass-through
data.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.port import Port


class HookRunReader(Port, Protocol):
    @abstractmethod
    async def run_ids_for_batch(
        self, ingest_run_id: str, batch_index: int
    ) -> dict[str, str]:
        """``{hook_name: hook_run_id}`` for one ingest batch (latest per hook)."""
        ...

    @abstractmethod
    async def run_ids_for_deposition(self, deposition_id: str) -> dict[str, str]:
        """``{hook_name: hook_run_id}`` for a deposition (latest per hook)."""
        ...
