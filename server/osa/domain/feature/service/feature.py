"""Feature service — manages feature tables and feature insertion."""

import logging
from typing import Any

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class FeatureService(Service):
    """Wraps FeatureStore port with domain logic for feature management."""

    feature_store: FeatureStore
    feature_storage: FeatureStoragePort

    async def create_table(self, hook: HookDefinition) -> None:
        """Create a feature table from a full HookDefinition."""
        await self.feature_store.create_table(
            hook.manifest.name, hook.manifest.feature_schema.columns
        )

    async def create_table_from_snapshot(self, snapshot: HookSnapshot) -> None:
        """Create a feature table from a HookSnapshot (event payload)."""
        await self.feature_store.create_table(snapshot.name, snapshot.features)

    async def insert_features(
        self,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert feature rows into the feature table. Returns row count."""
        return await self.feature_store.insert_features(hook_name, record_srn, rows)

    async def insert_features_for_record(
        self,
        deposition_srn: DepositionSRN,
        record_srn: str,
        hooks: list[HookSnapshot] | None = None,
    ) -> None:
        """Read hook features and insert into feature tables.

        If hooks are provided (from enriched event), iterate those.
        """
        if not hooks:
            return

        for hook_snapshot in hooks:
            hook_name = hook_snapshot.name
            if not await self.feature_storage.hook_features_exist(deposition_srn, hook_name):
                continue

            features = await self.feature_storage.read_hook_features(deposition_srn, hook_name)
            if features:
                count = await self.insert_features(
                    hook_name=hook_name,
                    record_srn=record_srn,
                    rows=features,
                )
                logger.debug(f"Inserted {count} features for hook={hook_name} record={record_srn}")
