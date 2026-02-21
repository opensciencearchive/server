"""Feature service — manages feature tables and feature insertion."""

from typing import Any

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.service import Service


class FeatureService(Service):
    """Wraps FeatureStore port with domain logic for feature management."""

    feature_store: FeatureStore

    async def create_tables(self, convention_id: str, hooks: list[HookDefinition]) -> None:
        """Create feature tables for each hook in a convention."""
        await self.feature_store.create_tables(convention_id, hooks)

    async def insert_features(
        self,
        convention_id: str,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert feature rows into the feature table. Returns row count."""
        return await self.feature_store.insert_features(convention_id, hook_name, record_srn, rows)
