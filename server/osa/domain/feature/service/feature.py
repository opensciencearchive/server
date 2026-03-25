"""Feature service — manages feature tables and feature insertion."""

import logging
from typing import Any

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class FeatureService(Service):
    """Wraps FeatureStore port with domain logic for feature management."""

    feature_store: FeatureStore
    feature_storage: FeatureStoragePort

    async def create_table(self, hook: HookDefinition) -> None:
        """Create a feature table from a HookDefinition."""
        await self.feature_store.create_table(hook.name, hook.feature.columns)

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
        hook_output_dir: str,
        record_srn: str,
        expected_features: list[str] | None = None,
    ) -> None:
        """Read hook features from the given directory and insert into feature tables.

        Warns (does not raise) when an expected feature is missing — the record
        is already published, blocking other features would be worse.
        """
        if not expected_features:
            return

        for feature_name in expected_features:
            if not await self.feature_storage.hook_features_exist(hook_output_dir, feature_name):
                logger.warning(
                    f"Expected feature '{feature_name}' not found in {hook_output_dir} "
                    f"for record {record_srn}"
                )
                continue

            features = await self.feature_storage.read_hook_features(hook_output_dir, feature_name)
            if features:
                count = await self.insert_features(
                    hook_name=feature_name,
                    record_srn=record_srn,
                    rows=features,
                )
                logger.info(
                    f"Inserted {count} features for hook={feature_name} record={record_srn}"
                )
