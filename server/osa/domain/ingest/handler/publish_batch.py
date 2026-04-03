"""PublishBatch — reads hook outputs, bulk-publishes passing records."""

from datetime import UTC, datetime
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.ingest.event.events import (
    HookBatchCompleted,
    IngestBatchPublished,
    IngestCompleted,
)
from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.model.ingester_record import IngesterRecord
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.ingest.port.storage import IngestStoragePort
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.service import RecordService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.source import IngestSource
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)


class PublishBatch(EventHandler[HookBatchCompleted]):
    """Reads hook outputs, constructs RecordDrafts, bulk-publishes passing records."""

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    record_service: RecordService
    feature_storage: FeatureStoragePort
    outbox: Outbox
    ingest_storage: IngestStoragePort

    async def handle(self, event: HookBatchCompleted) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_srn)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_srn}")

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(ingest_run.convention_srn)
        )

        # Read ingester records via storage port (filesystem or S3)
        raw_records = await self.ingest_storage.read_records(
            event.ingest_run_srn, event.batch_index
        )
        ingester_records = IngesterRecord.from_dicts(raw_records)

        # batch_dir used as locator for hook outcome reads
        batch_dir = str(self.ingest_storage.batch_dir(event.ingest_run_srn, event.batch_index))

        # Read hook outcomes for all hooks
        expected_features = [h.name for h in convention.hooks]

        # Determine which records passed all hooks (via storage port — works on filesystem + S3)
        # TODO: is this efficient, are we hitting S3 a lot?
        passed_records = await _get_passed_records(
            ingester_records=ingester_records,
            batch_dir=str(batch_dir),
            hooks=expected_features,
            feature_storage=self.feature_storage,
        )

        # Log outcome breakdown per hook
        short_id = event.ingest_run_srn.rsplit(":", 1)[-1][:8]
        total = len(ingester_records)
        for hook_name in expected_features:
            outcomes = await self.feature_storage.read_batch_outcomes(str(batch_dir), hook_name)
            from osa.domain.validation.model.batch_outcome import OutcomeStatus

            passed = sum(1 for o in outcomes.values() if o.status == OutcomeStatus.PASSED)
            rejected = sum(1 for o in outcomes.values() if o.status == OutcomeStatus.REJECTED)
            errored = sum(1 for o in outcomes.values() if o.status == OutcomeStatus.ERRORED)
            missing = total - len(outcomes)
            log.info(
                "[{short_id}] batch {batch_index} hook={hook_name}: "
                "{passed}/{total} passed, {rejected} rejected, {errored} errored, {missing} missing",
                short_id=short_id,
                batch_index=event.batch_index,
                hook_name=hook_name,
                total=total,
                passed=passed,
                rejected=rejected,
                errored=errored,
                missing=missing,
                ingest_run_srn=event.ingest_run_srn,
            )

        published_count = 0
        if passed_records:
            # Construct RecordDrafts
            drafts: list[RecordDraft] = []
            for record in passed_records:
                drafts.append(
                    RecordDraft(
                        source=IngestSource(
                            id=f"{ingest_run.convention_srn}:{record.source_id}",
                            ingest_run_srn=ingest_run.srn,
                            upstream_source=record.source_id,
                        ),
                        metadata=record.metadata,
                        convention_srn=ConventionSRN.parse(ingest_run.convention_srn),
                        expected_features=expected_features,
                    )
                )

            # Bulk publish — ON CONFLICT DO NOTHING skips duplicates
            published = await self.record_service.bulk_publish(drafts)
            published_srns = [str(r.srn) for r in published]
            published_count = len(published)

            # Build upstream ID → record SRN mapping for feature insertion
            upstream_to_record_srn: dict[str, str] = {}
            for record in published:
                # TODO: should we make RecordDraft generic over source type so we don't have to check this at runtime?
                if not isinstance(record.source, IngestSource):
                    log.warn(
                        "Skipping record with unsupported source type: {source_type}",
                        source_type=type(record.source).__name__,
                    )
                    continue
                upstream_to_record_srn[record.source.upstream_source] = str(record.srn)

            log.info(
                "[{short_id}] batch {batch_index}: published {published}/{passed} records ({duplicates} duplicates skipped)",
                short_id=short_id,
                batch_index=event.batch_index,
                published=published_count,
                passed=len(passed_records),
                duplicates=len(drafts) - published_count,
                ingest_run_srn=event.ingest_run_srn,
            )

            # Emit IngestBatchPublished for feature insertion
            if published_count > 0:
                await self.outbox.append(
                    IngestBatchPublished(
                        id=EventId(uuid4()),
                        ingest_run_srn=event.ingest_run_srn,
                        convention_srn=ingest_run.convention_srn,
                        batch_index=event.batch_index,
                        published_srns=published_srns,
                        published_count=published_count,
                        expected_features=expected_features,
                        upstream_to_record_srn=upstream_to_record_srn,
                    )
                )

        # Update counters atomically
        updated = await self.ingest_repo.increment_completed(
            event.ingest_run_srn,
            published_count=published_count,
        )

        # Check completion condition
        if updated.is_complete and updated.status == IngestStatus.RUNNING:
            updated.check_completion(datetime.now(UTC))
            await self.ingest_repo.save(updated)

            await self.outbox.append(
                IngestCompleted(
                    id=EventId(uuid4()),
                    ingest_run_srn=event.ingest_run_srn,
                    total_published=updated.published_count,
                )
            )
            short_id = event.ingest_run_srn.rsplit(":", 1)[-1][:8]
            log.info(
                "[{short_id}] COMPLETE: {total_published} records published",
                short_id=short_id,
                total_published=updated.published_count,
                ingest_run_srn=event.ingest_run_srn,
            )


async def _get_passed_records(
    ingester_records: list[IngesterRecord],
    batch_dir: str,
    hooks: list[str],
    feature_storage: FeatureStoragePort,
) -> list[IngesterRecord]:
    """Determine which records passed ALL hooks via the storage port."""
    if not hooks:
        return ingester_records

    passed_ids: set[str] | None = None

    for hook_name in hooks:
        outcomes = await feature_storage.read_batch_outcomes(batch_dir, hook_name)
        if not outcomes:
            return []
        from osa.domain.validation.model.batch_outcome import OutcomeStatus

        hook_passed = {rid for rid, o in outcomes.items() if o.status == OutcomeStatus.PASSED}

        if passed_ids is None:
            passed_ids = hook_passed
        else:
            passed_ids &= hook_passed

    if not passed_ids:
        return []

    return [r for r in ingester_records if r.source_id in passed_ids]
