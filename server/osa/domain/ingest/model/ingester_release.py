"""IngesterRelease — the immutable, versioned ingester artifact (#180).

The exact mirror of :class:`~osa.domain.validation.model.hook_release.HookRelease`,
and for the same reason: a release captures *what code* ran, so a row it produced
can name the image, digest, config and build ref that produced it.

Hooks have had this since #145 — every ``features.*`` row carries a ``run_id``
that resolves to a release. Records did not: the ingester's image and digest
lived inline on the convention as a mutable blob, overwritten on every redeploy,
so "which code asserted this metadata, at what version" was unanswerable. That
asymmetry is the core of #164. This type closes it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, NewType
from uuid import UUID

from pydantic import Field

from osa.domain.shared.model.entity import Entity
from osa.domain.shared.model.hook import OciConfig
from osa.domain.shared.model.source import IngesterName

IngesterReleaseId = NewType("IngesterReleaseId", UUID)


class IngesterRelease(Entity):
    """Immutable, versioned ingester artifact.

    ``runtime`` + ``source_ref`` are the deployable unit; ``version`` is
    monotonic per ``ingester_name``. Reuses :class:`OciConfig` rather than
    defining a parallel ingester runtime — "how a container runs" is one shape,
    and the ingester and hook runners consume it identically.
    """

    model_config = {"frozen": True}

    id: IngesterReleaseId
    ingester_name: IngesterName
    version: int
    runtime: Annotated[OciConfig, Field(discriminator="type")]
    source_ref: str
    built_by: str | None = None
    built_at: datetime


@dataclass(frozen=True)
class IngesterReleaseOutcome:
    """Result of minting a release.

    ``created`` is ``True`` when a new version was minted, ``False`` for an
    idempotent no-op (the digest already existed). It is decided *inside* the
    registry's row lock, so it is correct under concurrent identical
    submissions — the caller maps it to HTTP 201 vs 200 without a racy
    pre-check.
    """

    release: IngesterRelease
    created: bool
