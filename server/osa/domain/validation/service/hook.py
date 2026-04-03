"""HookService — executes hooks with OOM retry and checkpointing.

Handles both single-record (deposition) and multi-record (ingestion) batches.
On OOM, retries with doubled memory up to MAX_OOM_RETRIES times.
Checkpoints partial progress so crash recovery skips completed records.

Sorting assumption: hooks process records in input order and write output
incrementally (features.jsonl line by line). Sorting by file size ascending
maximizes checkpoint progress before a potential OOM on a large record.
"""

import json
from collections.abc import Iterable
from pathlib import Path

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.service import Service
from osa.domain.validation.model.batch_outcome import (
    BatchRecordOutcome,
    HookRecordId,
    OutcomeStatus,
)
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.domain.validation.port.storage import HookStoragePort
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)

MAX_OOM_RETRIES = 3


class HookService(Service):
    """Executes a hook with OOM retry, checkpointing, and finalization."""

    hook_runner: HookRunner
    hook_storage: HookStoragePort

    async def run_hook(
        self,
        hook: HookDefinition,
        inputs: HookInputs,
        work_dir: Path,
    ) -> HookResult:
        """Run a single hook against a batch of records, retrying on OOM.

        Returns the final HookResult. On success or non-OOM failure, returns
        after the first attempt. On OOM, retries with doubled memory up to
        MAX_OOM_RETRIES times, checkpointing partial progress between attempts.
        """
        records = inputs.records
        if not records:
            return HookResult(
                hook_name=hook.name,
                status=HookStatus.PASSED,
                duration_seconds=0.0,
            )

        # Load checkpoint (crash recovery)
        outcomes = _load_checkpoint(work_dir)
        remaining = _sort_by_size(r for r in records if r.id not in outcomes)

        if not remaining:
            # All records already checkpointed
            await self.hook_storage.write_batch_outcomes(work_dir, outcomes)
            return HookResult(
                hook_name=hook.name,
                status=HookStatus.PASSED,
                duration_seconds=0.0,
            )

        current_hook = hook
        total_duration = 0.0

        for attempt in range(1 + MAX_OOM_RETRIES):
            attempt_inputs = HookInputs(
                records=remaining,
                run_id=inputs.run_id,
                files_dirs=inputs.files_dirs,
                config=inputs.config,
            )

            result = await self.hook_runner.run(current_hook, attempt_inputs, work_dir)
            total_duration += result.duration_seconds

            # Read any output written by this attempt
            new_outcomes = _read_output_dir(work_dir)
            for rid, outcome in new_outcomes.items():
                if rid not in outcomes:
                    outcomes[rid] = outcome

            if result.oom_killed:
                # Checkpoint what we have so far
                await self.hook_storage.write_checkpoint(work_dir, outcomes)

                remaining = _sort_by_size(r for r in records if r.id not in outcomes)
                if not remaining:
                    break

                if attempt < MAX_OOM_RETRIES:
                    current_hook = current_hook.with_doubled_memory()
                    log.info(
                        "OOM retry {attempt}/{max_retries} for hook={hook_name}, memory={memory}, remaining={remaining} records",
                        attempt=attempt + 1,
                        max_retries=MAX_OOM_RETRIES,
                        hook_name=hook.name,
                        memory=current_hook.runtime.limits.memory,
                        remaining=len(remaining),
                    )
                    continue
                else:
                    # Exhausted retries — mark remaining as errored
                    for r in remaining:
                        outcomes[HookRecordId(r.id)] = BatchRecordOutcome(
                            record_id=HookRecordId(r.id),
                            status=OutcomeStatus.ERRORED,
                            error=f"OOM after {MAX_OOM_RETRIES} retries (last limit: {current_hook.runtime.limits.memory})",
                        )
                    await self.hook_storage.write_batch_outcomes(work_dir, outcomes)
                    return HookResult(
                        hook_name=hook.name,
                        status=HookStatus.OOM,
                        error_message=f"OOM exhausted after {MAX_OOM_RETRIES} retries",
                        duration_seconds=total_duration,
                    )
            elif result.status == HookStatus.FAILED:
                # Non-OOM failure — no retry
                await self.hook_storage.write_batch_outcomes(work_dir, outcomes)
                return result
            elif result.status == HookStatus.REJECTED:
                # Rejection — no retry, propagate status
                await self.hook_storage.write_batch_outcomes(work_dir, outcomes)
                return HookResult(
                    hook_name=hook.name,
                    status=HookStatus.REJECTED,
                    rejection_reason=result.rejection_reason,
                    duration_seconds=total_duration,
                )
            else:
                # Success (PASSED)
                break

        # Finalize: write canonical output files
        await self.hook_storage.write_batch_outcomes(work_dir, outcomes)
        _cleanup_checkpoint(work_dir)

        return HookResult(
            hook_name=hook.name,
            status=HookStatus.PASSED,
            duration_seconds=total_duration,
        )

    async def run_hooks_for_batch(
        self,
        hooks: list[HookDefinition],
        inputs: HookInputs,
        work_dirs: dict[str, Path],
    ) -> list[HookResult]:
        """Run multiple hooks sequentially for a batch of records.

        work_dirs maps hook_name → output directory.
        """
        results: list[HookResult] = []
        for hook in hooks:
            work_dir = work_dirs[hook.name]
            result = await self.run_hook(hook, inputs, work_dir)
            results.append(result)
        return results


def _sort_by_size(records: Iterable[HookRecord]) -> list[HookRecord]:
    """Sort records by size_hint_mb ascending. Skip sort when all sizes are 0."""
    record_list = list(records)
    if any(r.size_hint_mb > 0 for r in record_list):
        return sorted(record_list, key=lambda r: r.size_hint_mb)
    return record_list


def _load_checkpoint(work_dir: Path) -> dict[HookRecordId, BatchRecordOutcome]:
    """Load checkpoint from _checkpoint.jsonl. Returns empty dict on missing/corrupt."""
    checkpoint_path = work_dir / "_checkpoint.jsonl"
    if not checkpoint_path.exists():
        return {}

    outcomes: dict[HookRecordId, BatchRecordOutcome] = {}
    for line in checkpoint_path.open():
        line = line.strip()
        if not line:
            continue
        try:
            data = json.loads(line)
            outcome = BatchRecordOutcome.model_validate(data)
            outcomes[outcome.record_id] = outcome
        except (json.JSONDecodeError, ValueError):
            log.warn("Skipping malformed checkpoint line")
            continue

    return outcomes


def _read_output_dir(work_dir: Path) -> dict[HookRecordId, BatchRecordOutcome]:
    """Read hook output files (features.jsonl, rejections.jsonl, errors.jsonl)."""
    output_dir = work_dir / "output"
    outcomes: dict[HookRecordId, BatchRecordOutcome] = {}

    for filename, status, field_map in [
        ("features.jsonl", OutcomeStatus.PASSED, {"features": "features"}),
        ("rejections.jsonl", OutcomeStatus.REJECTED, {"reason": "reason"}),
        ("errors.jsonl", OutcomeStatus.ERRORED, {"error": "error", "retryable": "retryable"}),
    ]:
        path = output_dir / filename
        if not path.exists():
            continue
        for line in path.open():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue
            raw_id = data.get("id")
            if not raw_id:
                continue
            record_id = HookRecordId(raw_id)
            kwargs: dict = {"record_id": record_id, "status": status}
            for src, dst in field_map.items():
                if src in data:
                    kwargs[dst] = data[src]
            outcomes[record_id] = BatchRecordOutcome(**kwargs)

    return outcomes


def _cleanup_checkpoint(work_dir: Path) -> None:
    """Remove checkpoint file after successful finalization."""
    checkpoint_path = work_dir / "_checkpoint.jsonl"
    checkpoint_path.unlink(missing_ok=True)
