"""Serializable snapshot of hook configuration carried in event payloads.

Used across multiple domains' event payloads so consuming domains
have all data they need without querying the producing domain's repositories.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.value import ValueObject

if TYPE_CHECKING:
    from osa.domain.shared.model.hook import HookDefinition


class HookSnapshot(ValueObject):
    """Snapshot of hook configuration carried in event payloads.

    Contains the subset of HookDefinition data that downstream
    consumers (validation, feature, source) need to operate
    without cross-domain imports.
    """

    name: str
    image: str
    digest: str = ""
    features: list[ColumnDef] = []
    config: dict[str, Any] = {}

    @classmethod
    def from_definitions(cls, hooks: list[HookDefinition]) -> list[HookSnapshot]:
        """Convert HookDefinitions to HookSnapshots for event payloads."""
        return [
            cls(
                name=h.manifest.name,
                image=h.image,
                digest=h.digest,
                features=h.manifest.feature_schema.columns,
                config=h.config or {},
            )
            for h in hooks
        ]
