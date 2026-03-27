"""RunHooks — runs hook containers on an ingester batch."""

import json
from pathlib import Path
from uuid import uuid4

from osa.domain.deposition.service.convention import ConventionService
from osa.domain.ingest.event.events import HookBatchCompleted, IngesterBatchReady
from osa.domain.ingest.model.ingester_record import IngesterRecord
from osa.domain.ingest.port.repository import IngestRunRepository
from osa.domain.shared.error import NotFoundError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.infrastructure.logging import get_logger
from osa.infrastructure.storage.layout import StorageLayout

log = get_logger(__name__)


class RunHooks(EventHandler[IngesterBatchReady]):
    """Runs hook containers on an ingester batch and emits HookBatchCompleted."""

    __claim_timeout__ = 3600.0

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

        records = _read_ingester_records(records_file)

        if not records:
            log.warn(
                "ingest batch {batch_index}: no records to process",
                batch_index=event.batch_index,
                ingest_run_srn=event.ingest_run_srn,
            )

        # Build files_dirs from ingester files
        files_base = ingester_dir / "files"
        files_dirs: dict[str, Path] = {}
        if files_base.exists():
            for record in records:
                record_files = files_base / record.source_id
                if record_files.exists():
                    files_dirs[record.source_id] = record_files

        # Run each hook sequentially
        for hook in convention.hooks:
            hook_output_dir = self.layout.ingest_batch_hook_dir(
                event.ingest_run_srn, event.batch_index, hook.name
            )
            hook_output_dir.mkdir(parents=True, exist_ok=True)

            inputs = HookInputs(
                records=[HookRecord(id=r.source_id, metadata=r.metadata) for r in records],
                run_id=f"{event.ingest_run_srn}_batch{event.batch_index}",
                files_dirs=files_dirs,
                config=None,
            )

            result = await self.hook_runner.run(hook, inputs, hook_output_dir)

            short_id = event.ingest_run_srn.rsplit(":", 1)[-1][:8]
            log.info(
                "[{short_id}] batch {batch_index} hook={hook_name}: {status} in {duration:.1f}s",
                short_id=short_id,
                batch_index=event.batch_index,
                hook_name=hook.name,
                status=result.status.value,
                duration=result.duration_seconds,
                ingest_run_srn=event.ingest_run_srn,
            )

        # Emit HookBatchCompleted
        await self.outbox.append(
            HookBatchCompleted(
                id=EventId(uuid4()),
                ingest_run_srn=event.ingest_run_srn,
                batch_index=event.batch_index,
            )
        )


def _read_ingester_records(records_file: Path) -> list[IngesterRecord]:
    """Read ingester records from JSONL file into typed objects."""
    records: list[IngesterRecord] = []
    if not records_file.exists():
        return records
    for line in records_file.open():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            records.append(
                IngesterRecord(
                    source_id=data.get("source_id", data.get("id", "")),
                    metadata=data.get("metadata", {}),
                    file_paths=data.get("file_paths", []),
                )
            )
        except (json.JSONDecodeError, ValueError):
            log.warn("Skipping malformed ingester record line")
    return records
