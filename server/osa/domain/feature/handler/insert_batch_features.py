"""InsertBatchFeatures — bulk feature insertion for ingest batches."""

import logging

from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.ingest.event.events import IngestBatchPublished
from osa.domain.shared.event import EventHandler

logger = logging.getLogger(__name__)


class InsertBatchFeatures(EventHandler[IngestBatchPublished]):
    """Reads hook outputs for an ingest batch and inserts features in bulk.

    Handles IngestBatchPublished (batch-level event) rather than
    per-record RecordPublished. Shares core insertion logic with
    InsertRecordFeatures via FeatureService.
    """

    feature_service: FeatureService
    feature_storage: FeatureStoragePort

    async def handle(self, event: IngestBatchPublished) -> None:
        if not event.expected_features or not event.published_srns:
            return

        hook_output_root = self.feature_storage.get_hook_output_root("ingest", event.ingest_run_srn)

        # Read batch outcomes for each hook and insert features
        for record_srn in event.published_srns:
            await self.feature_service.insert_features_for_record(
                hook_output_dir=hook_output_root,
                record_srn=record_srn,
                expected_features=event.expected_features,
            )

        logger.info(
            "Inserted features for %d records in batch %d of %s",
            len(event.published_srns),
            event.batch_index,
            event.ingest_run_srn,
        )
