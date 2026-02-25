"""Unit tests for LocalFileStorageAdapter hook output methods."""

import json
from pathlib import Path

import pytest

from osa.domain.shared.model.srn import DepositionSRN
from osa.infrastructure.persistence.adapter.storage import LocalFileStorageAdapter


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


class TestGetHookOutputDir:
    def test_returns_hooks_subdirectory(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        output_dir = adapter.get_hook_output_dir(dep_srn, "pocket_detect")

        expected = tmp_path / "depositions" / "localhost_test-dep" / "hooks" / "pocket_detect"
        assert output_dir == expected
        assert output_dir.exists()

    def test_creates_directory(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        output_dir = adapter.get_hook_output_dir(dep_srn, "my_hook")

        assert output_dir.is_dir()

    def test_idempotent(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        dir1 = adapter.get_hook_output_dir(dep_srn, "hook_a")
        dir2 = adapter.get_hook_output_dir(dep_srn, "hook_a")

        assert dir1 == dir2


class TestReadHookFeatures:
    @pytest.mark.asyncio
    async def test_reads_features_list(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        # Write features.json in the output/ subdirectory
        output_dir = tmp_path / "depositions" / "localhost_test-dep" / "hooks" / "detect" / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "features.json").write_text(json.dumps([{"score": 0.95}, {"score": 0.82}]))

        features = await adapter.read_hook_features(dep_srn, "detect")

        assert len(features) == 2
        assert features[0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_reads_features_dict(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        output_dir = tmp_path / "depositions" / "localhost_test-dep" / "hooks" / "detect" / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "features.json").write_text(json.dumps({"score": 0.95}))

        features = await adapter.read_hook_features(dep_srn, "detect")

        assert len(features) == 1
        assert features[0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_returns_empty_when_missing(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        features = await adapter.read_hook_features(dep_srn, "nonexistent")

        assert features == []


class TestHookFeaturesExist:
    @pytest.mark.asyncio
    async def test_true_when_file_exists(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        output_dir = tmp_path / "depositions" / "localhost_test-dep" / "hooks" / "detect" / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "features.json").write_text("[]")

        assert await adapter.hook_features_exist(dep_srn, "detect") is True

    @pytest.mark.asyncio
    async def test_false_when_missing(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        assert await adapter.hook_features_exist(dep_srn, "nonexistent") is False


class TestDeleteCleansHookOutputs:
    @pytest.mark.asyncio
    async def test_rmtree_removes_hooks_dir(self, tmp_path: Path):
        adapter = LocalFileStorageAdapter(base_path=str(tmp_path))
        dep_srn = _make_dep_srn()

        # Create hook output
        hook_dir = tmp_path / "depositions" / "localhost_test-dep" / "hooks" / "detect"
        hook_dir.mkdir(parents=True)
        (hook_dir / "features.json").write_text("[]")

        await adapter.delete_files_for_deposition(dep_srn)

        assert not (tmp_path / "depositions" / "localhost_test-dep").exists()
