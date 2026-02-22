"""InsertRecordFeatures — deferred feature insertion on record publication."""

import logging

from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class InsertRecordFeatures(EventHandler[RecordPublished]):
    """Reads hook outputs from cold storage and inserts features with record_srn.

    Triggered after a record is published. Looks up the deposition's convention
    to find hook definitions, then reads features.json from each hook's output
    directory and inserts them into the feature tables.
    """

    deposition_repo: DepositionRepository
    convention_repo: ConventionRepository
    file_storage: FileStoragePort
    feature_service: FeatureService

    async def handle(self, event: RecordPublished) -> None:
        dep = await self.deposition_repo.get(event.deposition_srn)
        if dep is None:
            logger.error(f"Deposition not found: {event.deposition_srn}")
            return

        convention = await self.convention_repo.get(dep.convention_srn)
        if convention is None:
            logger.error(f"Convention not found: {dep.convention_srn}")
            return

        for hook_def in convention.hooks:
            hook_name = hook_def.manifest.name
            if not await self.file_storage.hook_features_exist(event.deposition_srn, hook_name):
                continue

            features = await self.file_storage.read_hook_features(event.deposition_srn, hook_name)
            if features:
                count = await self.feature_service.insert_features(
                    hook_name=hook_name,
                    record_srn=str(event.record_srn),
                    rows=features,
                )
                logger.debug(
                    f"Inserted {count} features for hook={hook_name} record={event.record_srn}"
                )
