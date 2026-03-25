"""InsertBatchFeatures — bulk feature insertion for ingest batches."""

import logging

from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.ingest.event.events import IngestBatchPublished
from osa.domain.shared.event import EventHandler
from osa.infrastructure.storage.layout import StorageLayout

logger = logging.getLogger(__name__)


class InsertBatchFeatures(EventHandler[IngestBatchPublished]):
    """Reads hook outputs for an ingest batch and inserts features in bulk.

    Handles IngestBatchPublished (batch-level event) rather than
    per-record RecordPublished. Uses read_batch_outcomes to parse
    the JSONL output format (not the single-record features.json).
    """

    feature_service: FeatureService
    feature_storage: FeatureStoragePort
    layout: StorageLayout

    async def handle(self, event: IngestBatchPublished) -> None:
        if not event.expected_features or not event.published_srns:
            return

        batch_output_dir = str(
            self.layout.ingest_batch_dir(event.ingest_run_srn, event.batch_index)
        )

        total_inserted = 0

        for hook_name in event.expected_features:
            # Read JSONL outcomes for this hook
            outcomes = await self.feature_storage.read_batch_outcomes(batch_output_dir, hook_name)

            # Insert features for each published record that passed this hook
            for record_id, outcome in outcomes.items():
                if outcome.status != "passed" or not outcome.features:
                    continue

                count = await self.feature_service.insert_features(
                    hook_name=hook_name,
                    record_srn=record_id,
                    rows=outcome.features,
                )
                total_inserted += count

        logger.info(
            "Inserted %d feature rows for batch %d of %s (%d hooks)",
            total_inserted,
            event.batch_index,
            event.ingest_run_srn,
            len(event.expected_features),
        )
