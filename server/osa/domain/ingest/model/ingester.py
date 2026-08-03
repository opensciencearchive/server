"""Ingester aggregate — stable identity, target schema, live pointer (#180).

Mirrors :class:`~osa.domain.validation.model.hook.Hook`. Where a hook owns a
fixed *feature contract*, an ingester owns the *schema it produces records for*;
both carry a ``live_release_id`` naming the currently active release, advanced
under a row lock by the registry adapter.

``schema_id`` is the bare schema id, not a versioned :class:`SchemaId`: an
ingester keeps producing records as its schema gains versions, so binding it to
one version would break on every field addition.
"""

from __future__ import annotations

from datetime import datetime

from osa.domain.ingest.model.ingester_release import IngesterReleaseId
from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.source import IngesterName
from osa.domain.shared.model.srn import LocalId


class Ingester(Aggregate):
    """Stable ingester identity + target schema + live-release pointer."""

    model_config = {"frozen": True}

    name: IngesterName
    schema_id: LocalId
    live_release_id: IngesterReleaseId | None = None
    created_at: datetime

    def with_live_release(self, release_id: IngesterReleaseId) -> "Ingester":
        """Return a copy whose live pointer references *release_id*."""
        return self.model_copy(update={"live_release_id": release_id})
