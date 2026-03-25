"""PublishBatch — reads hook outputs, bulk-publishes passing records."""

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import (
    HookBatchCompleted,
    IngestBatchPublished,
    IngestCompleted,
)
from osa.domain.ingest.model.ingest_run import IngestStatus
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.service import RecordService
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.source import IngestSource
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.util.paths import OSAPaths

logger = logging.getLogger(__name__)


class PublishBatch(EventHandler[HookBatchCompleted]):
    """Reads hook outputs, constructs RecordDrafts, bulk-publishes passing records."""

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    record_service: RecordService
    feature_storage: FeatureStoragePort
    outbox: Outbox
    paths: OSAPaths

    async def handle(self, event: HookBatchCompleted) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_srn)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_srn}")

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(ingest_run.convention_srn)
        )

        # Read source records from batch dir
        ingest_dir = self.paths.data_dir / "ingests" / _safe_srn(event.ingest_run_srn)
        batch_dir = ingest_dir / "batches" / str(event.batch_index)
        source_records = _read_source_records(batch_dir / "source" / "records.jsonl")

        # Read hook outcomes for all hooks
        expected_features = [h.name for h in convention.hooks]

        # Determine which records passed all hooks
        passed_records = _get_passed_records(
            source_records=source_records,
            batch_dir=batch_dir,
            hooks=expected_features,
            feature_storage=self.feature_storage,
        )

        if not passed_records:
            logger.info("No passing records in batch %d", event.batch_index)
        else:
            # Construct RecordDrafts
            drafts: list[RecordDraft] = []
            for record in passed_records:
                source_id = record.get("source_id", record.get("id", ""))
                drafts.append(
                    RecordDraft(
                        source=IngestSource(
                            id=f"{ingest_run.convention_srn}:{source_id}",
                            ingest_run_srn=ingest_run.srn,
                            upstream_source=source_id,
                        ),
                        metadata=record.get("metadata", {}),
                        convention_srn=ConventionSRN.parse(ingest_run.convention_srn),
                        expected_features=expected_features,
                    )
                )

            # Bulk publish
            published = await self.record_service.bulk_publish(drafts)
            published_srns = [str(r.srn) for r in published]
            published_count = len(published)

            logger.info(
                "Published %d records from batch %d of %s",
                published_count,
                event.batch_index,
                event.ingest_run_srn,
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
                    )
                )

        # Update counters atomically — use published_count or 0
        count = len(passed_records) if passed_records else 0
        updated = await self.ingest_repo.increment_completed(
            event.ingest_run_srn,
            published_count=count,
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
            logger.info(
                "Ingest completed: %s (total published: %d)",
                event.ingest_run_srn,
                updated.published_count,
            )


def _read_source_records(records_file: Path) -> list[dict]:
    """Read source records from JSONL file."""
    records: list[dict] = []
    if not records_file.exists():
        return records
    for line in records_file.open():
        line = line.strip()
        if not line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            logger.warning("Skipping malformed source record line")
    return records


def _get_passed_records(
    source_records: list[dict],
    batch_dir: Path,
    hooks: list[str],
    feature_storage: FeatureStoragePort,
) -> list[dict]:
    """Determine which records passed all hooks by reading features.jsonl."""
    if not hooks:
        return source_records

    # For simplicity, read the last hook's features.jsonl to get passed IDs
    # (hooks run sequentially, so the last hook's output has the final set)
    last_hook = hooks[-1]
    features_file = batch_dir / "hooks" / last_hook / "output" / "features.jsonl"
    if not features_file.exists():
        return []

    passed_ids: set[str] = set()
    for line in features_file.open():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            record_id = data.get("id")
            if record_id:
                passed_ids.add(record_id)
        except json.JSONDecodeError:
            logger.warning("Skipping malformed features.jsonl line")

    return [r for r in source_records if r.get("source_id", r.get("id", "")) in passed_ids]


def _safe_srn(srn: str) -> str:
    return srn.replace(":", "_").replace("@", "_")
