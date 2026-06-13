"""HookRelease — the immutable, versioned hook artifact (feature #145).

A release captures *what code* runs for a hook at a point in time: the OCI
runtime (image + digest + config + limits) plus the build source ref
(reproducibility anchor). Releases are integer-versioned, monotonic per hook,
and never mutated after creation. The live pointer on the :class:`Hook`
aggregate selects which release is currently active.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, NewType
from uuid import UUID

from pydantic import Field

from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.hook import (
    HookName,
    OciConfig,
    format_memory,
    parse_memory,
)

HookReleaseId = NewType("HookReleaseId", UUID)


class HookRelease(Entity):
    """Immutable, versioned hook artifact. ``runtime`` + ``source_ref`` are the
    deployable unit; ``version`` is monotonic per ``hook_name``."""

    model_config = {"frozen": True}

    id: HookReleaseId
    hook_name: HookName
    version: int
    runtime: Annotated[OciConfig, Field(discriminator="type")]
    source_ref: str
    built_by: str | None = None
    built_at: datetime

    def with_memory(self, memory: str) -> "HookRelease":
        """Return an in-memory copy with a different memory limit.

        Used only by the OOM-retry path to escalate memory for a single
        execution attempt; the persisted release is never changed.
        """
        new_limits = self.runtime.limits.model_copy(update={"memory": memory})
        new_runtime = self.runtime.model_copy(update={"limits": new_limits})
        return self.model_copy(update={"runtime": new_runtime})

    def with_doubled_memory(self) -> "HookRelease":
        """Return an in-memory copy with 2x the current memory limit."""
        doubled = format_memory(parse_memory(self.runtime.limits.memory) * 2)
        return self.with_memory(doubled)
