"""Hook aggregate — stable identity, fixed output contract, live pointer (#145).

A hook owns a fixed feature contract (columns + cardinality) and its feature
table; the contract never changes across releases. ``live_release_id`` points
at the currently active :class:`HookRelease`. The pointer is advanced/repointed
under a row lock by the registry adapter; this pure aggregate just holds state
and produces repointed copies.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from pydantic import Field

from osa.domain.shared.model.aggregate import Aggregate
from osa.domain.shared.model.hook import HookName, TableFeatureSpec
from osa.domain.validation.model.hook_release import HookReleaseId


class Hook(Aggregate):
    """Stable hook identity + fixed output contract + live-release pointer."""

    model_config = {"frozen": True}

    name: HookName
    feature: Annotated[TableFeatureSpec, Field(discriminator="kind")]
    live_release_id: HookReleaseId | None = None
    created_at: datetime

    def model_post_init(self, __context: object) -> None:
        # A hook name becomes a feature-table URL slot under /data/{schema}/.
        from osa.domain.shared.error import ReservedNameError
        from osa.domain.shared.model.reserved import RESERVED_NAMES

        if self.name.root in RESERVED_NAMES:
            raise ReservedNameError(self.name.root, "hook")

    def with_live_release(self, release_id: HookReleaseId) -> "Hook":
        """Return a copy whose live pointer references *release_id*."""
        return self.model_copy(update={"live_release_id": release_id})
