"""Feature service — manages feature tables and feature insertion."""

import logging
from typing import Any

from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.feature.port.feature_store import FeatureStore
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class FeatureService(Service):
    """Wraps FeatureStore port with domain logic for feature management."""

    feature_store: FeatureStore
    deposition_repo: DepositionRepository
    convention_repo: ConventionRepository
    file_storage: FileStoragePort

    async def create_table(self, hook: HookDefinition) -> None:
        """Create a feature table for a hook."""
        await self.feature_store.create_table(hook.manifest.name, hook)

    async def insert_features(
        self,
        hook_name: str,
        record_srn: str,
        rows: list[dict[str, Any]],
    ) -> int:
        """Insert feature rows into the feature table. Returns row count."""
        return await self.feature_store.insert_features(hook_name, record_srn, rows)

    async def insert_features_for_record(
        self, deposition_srn: DepositionSRN, record_srn: str
    ) -> None:
        """Look up deposition, convention, read hook features, and insert."""
        dep = await self.deposition_repo.get(deposition_srn)
        if dep is None:
            logger.error(f"Deposition not found: {deposition_srn}")
            return

        convention = await self.convention_repo.get(dep.convention_srn)
        if convention is None:
            logger.error(f"Convention not found: {dep.convention_srn}")
            return

        for hook_def in convention.hooks:
            hook_name = hook_def.manifest.name
            if not await self.file_storage.hook_features_exist(deposition_srn, hook_name):
                continue

            features = await self.file_storage.read_hook_features(deposition_srn, hook_name)
            if features:
                count = await self.insert_features(
                    hook_name=hook_name,
                    record_srn=record_srn,
                    rows=features,
                )
                logger.debug(f"Inserted {count} features for hook={hook_name} record={record_srn}")
