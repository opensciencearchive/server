"""Tests for HookService — OOM retry with checkpointing."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.validation.model.batch_outcome import (
    BatchRecordOutcome,
    HookRecordId,
    OutcomeStatus,
)
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs


def _make_hook(name: str = "detect_pockets", memory: str = "1g") -> HookDefinition:
    return HookDefinition(
        name=name,
        runtime=OciConfig(
            image="img:v1",
            digest="sha256:abc",
            limits=OciLimits(memory=memory),
        ),
        feature=TableFeatureSpec(
            cardinality="one",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


def _inputs(records: list[HookRecord]) -> HookInputs:
    return HookInputs(records=records, run_id="test-run")


def _make_records(count: int = 3) -> list[HookRecord]:
    return [HookRecord(id=f"rec{i}", metadata={"title": f"Record {i}"}) for i in range(count)]


def _passed_result(hook_name: str = "detect_pockets", duration: float = 5.0) -> HookResult:
    return HookResult(hook_name=hook_name, status=HookStatus.PASSED, duration_seconds=duration)


def _oom_result(hook_name: str = "detect_pockets", duration: float = 30.0) -> HookResult:
    return HookResult(
        hook_name=hook_name,
        status=HookStatus.OOM,
        error_message="Hook killed by OOM",
        duration_seconds=duration,
    )


def _failed_result(hook_name: str = "detect_pockets", duration: float = 10.0) -> HookResult:
    return HookResult(
        hook_name=hook_name,
        status=HookStatus.FAILED,
        error_message="Some error",
        duration_seconds=duration,
    )


class FakeHookStorage:
    """Fake HookStoragePort for testing — stores checkpoints and outcomes in memory."""

    def __init__(self) -> None:
        self.checkpoints: dict[str, dict[HookRecordId, BatchRecordOutcome]] = {}
        self.written_outcomes: dict[str, dict[HookRecordId, BatchRecordOutcome]] = {}
        self._batch_outcomes: dict[str, dict[HookRecordId, BatchRecordOutcome]] = {}

    def get_hook_output_dir(self, deposition_srn: Any, hook_name: str) -> Path:
        return Path(f"/fake/hooks/{hook_name}")

    def get_files_dir(self, deposition_id: Any) -> Path:
        return Path("/fake/files")

    def write_checkpoint(
        self, work_dir: Path, outcomes: dict[HookRecordId, BatchRecordOutcome]
    ) -> None:
        self.checkpoints[str(work_dir)] = dict(outcomes)

    def write_batch_outcomes(
        self, work_dir: Path, outcomes: dict[HookRecordId, BatchRecordOutcome]
    ) -> None:
        self.written_outcomes[str(work_dir)] = dict(outcomes)

    async def read_batch_outcomes(
        self, output_dir: str, hook_name: str
    ) -> dict[HookRecordId, BatchRecordOutcome]:
        key = f"{output_dir}/{hook_name}"
        return self._batch_outcomes.get(key, {})

    def read_checkpoint(self, work_dir: Path) -> dict[HookRecordId, BatchRecordOutcome]:
        return self.checkpoints.get(str(work_dir), {})


class TestHookServiceNoOOM:
    """T015: No OOM — hook runs once, correct output."""

    @pytest.mark.asyncio
    async def test_no_oom_runs_once(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        records = _make_records(2)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()

        runner = AsyncMock()
        runner.run.return_value = _passed_result()
        storage = FakeHookStorage()

        # Simulate runner writing outcomes (features.jsonl)
        # After run, HookService reads output dir for outcomes
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        import json

        features_file = output_dir / "features.jsonl"
        features_file.write_text(
            "\n".join(json.dumps({"id": r.id, "features": [{"score": 0.9}]}) for r in records)
            + "\n"
        )

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.PASSED
        runner.run.assert_called_once()


class TestHookServiceOOMRetry:
    """T016: OOM retry doubles memory."""

    @pytest.mark.asyncio
    async def test_oom_retry_doubles_memory(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook(memory="1g")
        records = _make_records(2)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        import json

        call_count = 0

        async def mock_run(h, inputs, wd):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # First call: write partial output then OOM
                features_file = output_dir / "features.jsonl"
                features_file.write_text(
                    json.dumps({"id": records[0].id, "features": [{"score": 0.5}]}) + "\n"
                )
                return _oom_result()
            else:
                # Second call: succeed with remaining
                features_file = output_dir / "features.jsonl"
                # Append the second record
                with features_file.open("a") as f:
                    f.write(json.dumps({"id": records[1].id, "features": [{"score": 0.8}]}) + "\n")
                return _passed_result()

        runner = AsyncMock()
        runner.run.side_effect = mock_run
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.PASSED
        assert runner.run.call_count == 2
        # Second call should have doubled memory
        second_call_hook = runner.run.call_args_list[1][0][0]
        assert second_call_hook.runtime.limits.memory == "2g"


class TestHookServiceOOMExhaustion:
    """T017: OOM exhaustion marks remaining records as errored."""

    @pytest.mark.asyncio
    async def test_oom_exhaustion_marks_errored(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook(memory="1g")
        records = _make_records(1)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        runner = AsyncMock()
        runner.run.return_value = _oom_result()
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.OOM
        # Should have retried MAX_OOM_RETRIES times
        assert runner.run.call_count == 4  # 1 initial + 3 retries

        # Check outcomes written with error
        assert str(work_dir) in storage.written_outcomes
        outcomes = storage.written_outcomes[str(work_dir)]
        assert HookRecordId("rec0") in outcomes
        assert outcomes[HookRecordId("rec0")].status == OutcomeStatus.ERRORED
        assert "OOM" in (outcomes[HookRecordId("rec0")].error or "")


class TestHookServiceNonOOMFailure:
    """T018: Non-OOM failure does NOT trigger retry."""

    @pytest.mark.asyncio
    async def test_non_oom_failure_no_retry(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        records = _make_records(1)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        (work_dir / "output").mkdir(parents=True)

        runner = AsyncMock()
        runner.run.return_value = _failed_result()
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.FAILED
        runner.run.assert_called_once()


class TestHookServiceFinalize:
    """T019: Finalize writes canonical files."""

    @pytest.mark.asyncio
    async def test_finalize_writes_canonical_files(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        records = _make_records(2)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        import json

        features_file = output_dir / "features.jsonl"
        features_file.write_text(
            "\n".join(json.dumps({"id": r.id, "features": [{"score": 0.9}]}) for r in records)
            + "\n"
        )

        runner = AsyncMock()
        runner.run.return_value = _passed_result()
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        await service.run_hook(hook, _inputs(records), work_dir)

        assert str(work_dir) in storage.written_outcomes
        outcomes = storage.written_outcomes[str(work_dir)]
        assert len(outcomes) == 2
        for r in records:
            assert HookRecordId(r.id) in outcomes
            assert outcomes[HookRecordId(r.id)].status == OutcomeStatus.PASSED


class TestHookServiceEmptyRecords:
    """T020: Empty records list — no container launched."""

    @pytest.mark.asyncio
    async def test_empty_records_noop(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()

        runner = AsyncMock()
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs([]), work_dir)

        assert result.status == HookStatus.PASSED
        runner.run.assert_not_called()


class TestHookServiceMultiHook:
    """T021: Multi-hook — second OOMs, first not re-run."""

    @pytest.mark.asyncio
    async def test_multi_hook_second_ooms(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook1 = _make_hook(name="hook_one")
        hook2 = _make_hook(name="hook_two", memory="512m")
        records = _make_records(1)

        work_dir1 = tmp_path / "hook_one"
        work_dir1.mkdir()
        (work_dir1 / "output").mkdir(parents=True)
        work_dir2 = tmp_path / "hook_two"
        work_dir2.mkdir()
        (work_dir2 / "output").mkdir(parents=True)

        import json

        # Hook 1 succeeds
        (work_dir1 / "output" / "features.jsonl").write_text(
            json.dumps({"id": "rec0", "features": [{"score": 0.9}]}) + "\n"
        )

        runner = AsyncMock()
        storage = FakeHookStorage()

        call_index = 0

        async def side_effect(h, inputs, wd):
            nonlocal call_index
            call_index += 1
            if h.name == "hook_one":
                return _passed_result(hook_name="hook_one")
            else:
                return _oom_result(hook_name="hook_two")

        runner.run.side_effect = side_effect

        service = HookService(hook_runner=runner, hook_storage=storage)

        # Run hook 1 — should pass
        r1 = await service.run_hook(hook1, _inputs(records), work_dir1)
        assert r1.status == HookStatus.PASSED

        # Run hook 2 — should OOM and exhaust retries
        r2 = await service.run_hook(hook2, _inputs(records), work_dir2)
        assert r2.status == HookStatus.OOM

        # Hook 1 was called once, hook 2 was called 4 times (1 + 3 retries)
        hook1_calls = [c for c in runner.run.call_args_list if c[0][0].name == "hook_one"]
        hook2_calls = [c for c in runner.run.call_args_list if c[0][0].name == "hook_two"]
        assert len(hook1_calls) == 1
        assert len(hook2_calls) == 4


class TestHookServiceCheckpointRecovery:
    """T022: Crash recovery from checkpoint — skips completed records."""

    @pytest.mark.asyncio
    async def test_checkpoint_crash_recovery(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        records = _make_records(3)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        # Pre-populate checkpoint: rec0 already done
        import json

        checkpoint_file = work_dir / "_checkpoint.jsonl"
        checkpoint_file.write_text(
            json.dumps(
                {
                    "record_id": "rec0",
                    "status": "passed",
                    "features": [{"score": 0.5}],
                }
            )
            + "\n"
        )

        runner = AsyncMock()
        storage = FakeHookStorage()

        async def mock_run(h, inputs, wd):
            # Should only receive rec1 and rec2, not rec0
            input_ids = [r.id for r in inputs.records]
            assert "rec0" not in input_ids
            # Write output for remaining
            features_file = output_dir / "features.jsonl"
            with features_file.open("a") as f:
                for r in inputs.records:
                    f.write(json.dumps({"id": r.id, "features": [{"score": 0.9}]}) + "\n")
            return _passed_result()

        runner.run.side_effect = mock_run

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.PASSED
        runner.run.assert_called_once()
        # Final outcomes should contain all 3 records
        outcomes = storage.written_outcomes[str(work_dir)]
        assert len(outcomes) == 3


class TestHookServiceCheckpointAllComplete:
    """T023: All records in checkpoint — hook never called."""

    @pytest.mark.asyncio
    async def test_checkpoint_all_complete_skips_hook(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        records = _make_records(2)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()

        import json

        checkpoint_file = work_dir / "_checkpoint.jsonl"
        lines = []
        for r in records:
            lines.append(
                json.dumps({"record_id": r.id, "status": "passed", "features": [{"score": 0.9}]})
            )
        checkpoint_file.write_text("\n".join(lines) + "\n")

        runner = AsyncMock()
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.PASSED
        runner.run.assert_not_called()


class TestHookServiceSorting:
    """T034-T035: Records sorted by size_hint_mb ascending."""

    @pytest.mark.asyncio
    async def test_records_sorted_by_file_size(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        # Create records with different sizes — large first to test reordering
        records = [
            HookRecord(id="large", metadata={}, size_hint_mb=100.0),
            HookRecord(id="small", metadata={}, size_hint_mb=1.0),
            HookRecord(id="medium", metadata={}, size_hint_mb=50.0),
        ]
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        import json

        captured_order: list[str] = []

        async def mock_run(h, inputs, wd):
            for r in inputs.records:
                captured_order.append(r.id)
            features_file = output_dir / "features.jsonl"
            with features_file.open("w") as f:
                for r in inputs.records:
                    f.write(json.dumps({"id": r.id, "features": [{"score": 0.9}]}) + "\n")
            return _passed_result()

        runner = AsyncMock()
        runner.run.side_effect = mock_run
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        await service.run_hook(hook, _inputs(records), work_dir)

        assert captured_order == ["small", "medium", "large"]

    @pytest.mark.asyncio
    async def test_sorting_skipped_when_no_sizes(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        # All records have default size_hint_mb=0 — original order preserved
        records = [
            HookRecord(id="a", metadata={}),
            HookRecord(id="b", metadata={}),
            HookRecord(id="c", metadata={}),
        ]
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        import json

        captured_order: list[str] = []

        async def mock_run(h, inputs, wd):
            for r in inputs.records:
                captured_order.append(r.id)
            features_file = output_dir / "features.jsonl"
            with features_file.open("w") as f:
                for r in inputs.records:
                    f.write(json.dumps({"id": r.id, "features": [{"score": 0.9}]}) + "\n")
            return _passed_result()

        runner = AsyncMock()
        runner.run.side_effect = mock_run
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        await service.run_hook(hook, _inputs(records), work_dir)

        assert captured_order == ["a", "b", "c"]


class TestHookServiceCorruptedCheckpoint:
    """T024: Corrupted checkpoint treated as empty — all records reprocessed."""

    @pytest.mark.asyncio
    async def test_corrupted_checkpoint_treated_as_empty(self, tmp_path: Path):
        from osa.domain.validation.service.hook import HookService

        hook = _make_hook()
        records = _make_records(2)
        work_dir = tmp_path / "hook_out"
        work_dir.mkdir()
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)

        # Write corrupted checkpoint
        checkpoint_file = work_dir / "_checkpoint.jsonl"
        checkpoint_file.write_text("NOT VALID JSON\n{also broken\n")

        import json

        runner = AsyncMock()

        async def mock_run(h, inputs, wd):
            # Should receive ALL records since checkpoint is corrupted
            assert len(inputs.records) == 2
            features_file = output_dir / "features.jsonl"
            with features_file.open("w") as f:
                for r in inputs.records:
                    f.write(json.dumps({"id": r.id, "features": [{"score": 0.9}]}) + "\n")
            return _passed_result()

        runner.run.side_effect = mock_run
        storage = FakeHookStorage()

        service = HookService(hook_runner=runner, hook_storage=storage)
        result = await service.run_hook(hook, _inputs(records), work_dir)

        assert result.status == HookStatus.PASSED
        runner.run.assert_called_once()
