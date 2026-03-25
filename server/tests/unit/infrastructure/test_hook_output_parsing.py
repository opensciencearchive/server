"""T004: Unit tests for JSONL batch output parsing via FilesystemStorageAdapter."""

import json
from pathlib import Path

import pytest

from osa.infrastructure.persistence.adapter.storage import FilesystemStorageAdapter


def _write_jsonl(path: Path, lines: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")


def _hook_output_dir(base: Path, hook_name: str = "validate_dna") -> Path:
    """Create the expected directory structure: {base}/hooks/{hook_name}/output/"""
    d = base / "hooks" / hook_name / "output"
    d.mkdir(parents=True, exist_ok=True)
    return d


@pytest.fixture
def adapter(tmp_path: Path) -> FilesystemStorageAdapter:
    return FilesystemStorageAdapter(str(tmp_path))


HOOK = "validate_dna"


class TestReadBatchOutcomes:
    """Parse features.jsonl, rejections.jsonl, errors.jsonl via storage adapter."""

    @pytest.mark.anyio
    async def test_single_line_features(
        self, adapter: FilesystemStorageAdapter, tmp_path: Path
    ) -> None:
        output = _hook_output_dir(tmp_path)
        _write_jsonl(output / "features.jsonl", [{"id": "rec1", "features": [{"score": 0.9}]}])
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert len(outcomes) == 1
        assert outcomes["rec1"].status == "passed"
        assert outcomes["rec1"].features == [{"score": 0.9}]

    @pytest.mark.anyio
    async def test_multi_line_features(
        self, adapter: FilesystemStorageAdapter, tmp_path: Path
    ) -> None:
        output = _hook_output_dir(tmp_path)
        _write_jsonl(
            output / "features.jsonl",
            [
                {"id": "rec1", "features": [{"score": 0.9}]},
                {"id": "rec2", "features": [{"score": 0.7}]},
            ],
        )
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert len(outcomes) == 2
        assert outcomes["rec1"].status == "passed"
        assert outcomes["rec2"].status == "passed"

    @pytest.mark.anyio
    async def test_rejections(self, adapter: FilesystemStorageAdapter, tmp_path: Path) -> None:
        output = _hook_output_dir(tmp_path)
        _write_jsonl(
            output / "rejections.jsonl", [{"id": "rec3", "reason": "Missing required field"}]
        )
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert outcomes["rec3"].status == "rejected"
        assert outcomes["rec3"].reason == "Missing required field"

    @pytest.mark.anyio
    async def test_errors(self, adapter: FilesystemStorageAdapter, tmp_path: Path) -> None:
        output = _hook_output_dir(tmp_path)
        _write_jsonl(output / "errors.jsonl", [{"id": "rec4", "error": "OOM", "retryable": True}])
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert outcomes["rec4"].status == "errored"
        assert outcomes["rec4"].error == "OOM"

    @pytest.mark.anyio
    async def test_mixed_outcomes(self, adapter: FilesystemStorageAdapter, tmp_path: Path) -> None:
        output = _hook_output_dir(tmp_path)
        _write_jsonl(output / "features.jsonl", [{"id": "a", "features": []}])
        _write_jsonl(output / "rejections.jsonl", [{"id": "b", "reason": "bad"}])
        _write_jsonl(output / "errors.jsonl", [{"id": "c", "error": "fail"}])
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert len(outcomes) == 3
        assert outcomes["a"].status == "passed"
        assert outcomes["b"].status == "rejected"
        assert outcomes["c"].status == "errored"

    @pytest.mark.anyio
    async def test_empty_directory(self, adapter: FilesystemStorageAdapter, tmp_path: Path) -> None:
        _hook_output_dir(tmp_path)
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert outcomes == {}

    @pytest.mark.anyio
    async def test_malformed_json_line_skipped(
        self, adapter: FilesystemStorageAdapter, tmp_path: Path
    ) -> None:
        output = _hook_output_dir(tmp_path)
        (output / "features.jsonl").write_text(
            '{"id": "ok", "features": []}\n'
            "not valid json\n"
            '{"id": "also_ok", "features": [{"x": 1}]}\n'
        )
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert len(outcomes) == 2
        assert "ok" in outcomes
        assert "also_ok" in outcomes

    @pytest.mark.anyio
    async def test_missing_id_field_skipped(
        self, adapter: FilesystemStorageAdapter, tmp_path: Path
    ) -> None:
        output = _hook_output_dir(tmp_path)
        (output / "features.jsonl").write_text(
            '{"id": "ok", "features": []}\n{"features": [{"x": 1}]}\n'
        )
        outcomes = await adapter.read_batch_outcomes(str(tmp_path), HOOK)
        assert len(outcomes) == 1
        assert "ok" in outcomes

    @pytest.mark.anyio
    async def test_nonexistent_output_dir(
        self, adapter: FilesystemStorageAdapter, tmp_path: Path
    ) -> None:
        outcomes = await adapter.read_batch_outcomes(str(tmp_path / "nonexistent"), HOOK)
        assert outcomes == {}
