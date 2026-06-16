"""Feature service — manages feature tables and feature insertion."""

import logging
from typing import Any

from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.shared.model.hook import FeatureName, HookIdentity
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class FeatureService(Service):
    """Wraps FeatureStore port with domain logic for feature management."""

    feature_store: FeatureStore
    feature_storage: FeatureStoragePort

    async def create_table(self, hook: HookIdentity) -> None:
        """Create a feature table for a hook's output (named by the hook)."""
        await self.feature_store.create_table(hook.name.root, hook.feature.columns)

    async def insert_features(
        self,
        feature: FeatureName,
        record_srn: str,
        rows: list[dict[str, Any]],
        run_id: str,
    ) -> int:
        """Insert feature rows into the feature table. Returns row count.

        ``run_id`` is the ``hook_runs.id`` that produced these rows (provenance).
        """
        return await self.feature_store.insert_features(feature.root, record_srn, rows, run_id)

    async def insert_features_for_record(
        self,
        hook_output_dir: str,
        record_srn: str,
        expected_features: list[FeatureName] | None = None,
    ) -> None:
        """Read a record's hook outputs from storage and insert them into feature tables.

        Each feature's ``run_id`` is read from the ``run.json`` the producing run
        wrote into that hook's output dir (design-revisions §6). Warns (does not
        raise) when an expected feature is missing — the record is already
        published, blocking other features would be worse.
        """
        if not expected_features:
            return

        for feature in expected_features:
            name = feature.root
            if not await self.feature_storage.hook_features_exist(hook_output_dir, name):
                logger.warning(
                    f"Expected feature '{name}' not found in {hook_output_dir} "
                    f"for record {record_srn}"
                )
                continue

            run_ref = await self.feature_storage.read_run_ref(hook_output_dir, name)
            if run_ref is None:
                logger.warning(
                    f"No run.json for feature '{name}' in {hook_output_dir} "
                    f"for record {record_srn}; skipping (no provenance)"
                )
                continue

            rows = await self.feature_storage.read_hook_features(hook_output_dir, name)
            if rows:
                count = await self.insert_features(
                    feature=feature,
                    record_srn=record_srn,
                    rows=rows,
                    run_id=run_ref.run_id,
                )
                logger.info(f"Inserted {count} features for feature={name} record={record_srn}")
