"""RunHooks — runs hook containers on an ingester batch."""

import json
import logging
from pathlib import Path
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.infrastructure.storage.layout import StorageLayout

logger = logging.getLogger(__name__)


class RunHooks(EventHandler[IngesterBatchReady]):
    """Runs hook containers on an ingester batch and emits HookBatchCompleted."""

    __claim_timeout__ = 3600.0  # Hook runs can be long

    ingest_repo: IngestRunRepository
    convention_service: ConventionService
    hook_runner: HookRunner
    outbox: Outbox
    layout: StorageLayout

    async def handle(self, event: IngesterBatchReady) -> None:
        ingest_run = await self.ingest_repo.get(event.ingest_run_srn)
        if ingest_run is None:
            raise NotFoundError(f"Ingest run not found: {event.ingest_run_srn}")

        convention = await self.convention_service.get_convention(
            ConventionSRN.parse(ingest_run.convention_srn)
        )

        # Read records from batch ingester dir
        ingester_dir = self.layout.ingest_batch_ingester_dir(
            event.ingest_run_srn, event.batch_index
        )
        records_file = ingester_dir / "records.jsonl"

        records: list[dict] = []
        if records_file.exists():
            for line in records_file.open():
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        logger.warning(
                            "Skipping malformed record line in batch %d", event.batch_index
                        )

        if not records:
            logger.warning("No records in batch %d for %s", event.batch_index, event.ingest_run_srn)

        # Build files_dirs from ingester files
        files_base = ingester_dir / "files"
        files_dirs: dict[str, Path] = {}
        if files_base.exists():
            for record in records:
                record_id = record.get("source_id", record.get("id", ""))
                record_files = files_base / str(record_id)
                if record_files.exists():
                    files_dirs[str(record_id)] = record_files

        # Run each hook sequentially
        for hook in convention.hooks:
            hook_output_dir = self.layout.ingest_batch_hook_dir(
                event.ingest_run_srn, event.batch_index, hook.name
            )
            hook_output_dir.mkdir(parents=True, exist_ok=True)

            inputs = HookInputs(
                records=[
                    HookRecord(
                        id=r.get("source_id", r.get("id", "")),
                        metadata=r.get("metadata", {}),
                    )
                    for r in records
                ],
                run_id=f"{event.ingest_run_srn}_batch{event.batch_index}",
                files_dirs=files_dirs,
                config=None,
            )

            await self.hook_runner.run(hook, inputs, hook_output_dir)

        # Emit HookBatchCompleted
        await self.outbox.append(
            HookBatchCompleted(
                id=EventId(uuid4()),
                ingest_run_srn=event.ingest_run_srn,
                batch_index=event.batch_index,
            )
        )

        logger.info(
            "Hooks completed for batch %d of %s (%d records)",
            event.batch_index,
            event.ingest_run_srn,
            len(records),
        )
