"""InsertBatchFeatures — bulk feature insertion for ingest batches."""

from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.ingest.event.events import IngestBatchPublished
from osa.domain.shared.event import EventHandler
from osa.infrastructure.logging import get_logger
from osa.infrastructure.storage.layout import StorageLayout

log = get_logger(__name__)


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

        batch_output_dir = str(self.layout.ingest_batch_dir(event.ingest_run_id, event.batch_index))

        total_inserted = 0
        skipped_dupes = 0

        for hook_name in event.expected_features:
            # Read JSONL outcomes for this hook
            outcomes = await self.feature_storage.read_batch_outcomes(batch_output_dir, hook_name)

            # Insert features for each published record that passed this hook.
            # Map upstream source ID → published record SRN so features
            # are keyed by the record SRN, not the upstream ID.
            for upstream_id, outcome in outcomes.items():
                if outcome.status != "passed" or not outcome.features:
                    continue

                record_srn = event.upstream_to_record_srn.get(upstream_id)
                if not record_srn:
                    # Expected for cross-batch duplicates — the record was already
                    # published in an earlier batch, so ON CONFLICT DO NOTHING
                    # skipped it and features were already inserted then.
                    skipped_dupes += 1
                    continue

                count = await self.feature_service.insert_features(
                    hook_name=hook_name,
                    record_srn=record_srn,
                    rows=outcome.features,
                )
                total_inserted += count

        short_id = event.ingest_run_id[:8]
        dupe_msg = f", {skipped_dupes} duplicates skipped" if skipped_dupes else ""
        log.info(
            "[{short_id}] batch {batch_index}: inserted {total_inserted} feature rows ({hook_count} hooks{dupe_msg})",
            short_id=short_id,
            batch_index=event.batch_index,
            total_inserted=total_inserted,
            hook_count=len(event.expected_features),
            dupe_msg=dupe_msg,
            ingest_run_id=event.ingest_run_id,
        )
