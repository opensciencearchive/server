"""Unit tests for OciHookRunner — container lifecycle, parsing, and feature collection."""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookLimits,
    HookManifest,
)
from osa.domain.validation.model.hook_result import HookStatus, ProgressEntry
from osa.domain.validation.port.hook_runner import HookInputs
from osa.infrastructure.oci.runner import OciHookRunner


def _make_hook(
    name: str = "pocket_detect",
    timeout: int = 300,
    memory: str = "2g",
    cpu: str = "2.0",
    config: dict | None = None,
) -> HookDefinition:
    return HookDefinition(
        image="ghcr.io/example/hook:v1",
        digest="sha256:abc123",
        manifest=HookManifest(
            name=name,
            record_schema="Sample",
            cardinality="many",
            feature_schema=FeatureSchema(
                columns=[
                    ColumnDef(name="score", json_type="number", required=True),
                ]
            ),
        ),
        limits=HookLimits(timeout_seconds=timeout, memory=memory, cpu=cpu),
        config=config,
    )


def _make_runner(docker: AsyncMock | None = None) -> OciHookRunner:
    return OciHookRunner(docker=docker or AsyncMock())


class TestParseMemory:
    def test_gigabytes(self):
        runner = _make_runner()
        assert runner._parse_memory("2g") == 2 * 1024 * 1024 * 1024

    def test_megabytes(self):
        runner = _make_runner()
        assert runner._parse_memory("512m") == 512 * 1024 * 1024

    def test_kilobytes(self):
        runner = _make_runner()
        assert runner._parse_memory("1024k") == 1024 * 1024

    def test_bare_bytes(self):
        runner = _make_runner()
        assert runner._parse_memory("1048576") == 1048576

    def test_fractional(self):
        runner = _make_runner()
        assert runner._parse_memory("1.5g") == int(1.5 * 1024 * 1024 * 1024)

    def test_case_insensitive(self):
        runner = _make_runner()
        assert runner._parse_memory("2G") == 2 * 1024 * 1024 * 1024

    def test_with_i_suffix(self):
        runner = _make_runner()
        assert runner._parse_memory("2gi") == 2 * 1024 * 1024 * 1024

    def test_invalid_format(self):
        runner = _make_runner()
        with pytest.raises(ValueError, match="Invalid memory format"):
            runner._parse_memory("abc")


class TestParseProgress:
    def test_empty_when_no_file(self, tmp_path: Path):
        runner = _make_runner()
        entries = runner._parse_progress(tmp_path)
        assert entries == []

    def test_parses_valid_jsonl(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text(
            '{"step":"Loading","status":"completed","message":"Done"}\n'
            '{"step":"Analyzing","status":"completed","message":"Finished"}\n'
        )
        runner = _make_runner()
        entries = runner._parse_progress(tmp_path)
        assert len(entries) == 2
        assert entries[0].step == "Loading"
        assert entries[0].status == "completed"
        assert entries[0].message == "Done"

    def test_skips_invalid_json_lines(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text(
            '{"step":"Good","status":"completed"}\n'
            "not valid json\n"
            '{"step":"AlsoGood","status":"completed"}\n'
        )
        runner = _make_runner()
        entries = runner._parse_progress(tmp_path)
        assert len(entries) == 2

    def test_skips_blank_lines(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text(
            '{"step":"A","status":"completed"}\n\n{"step":"B","status":"completed"}\n'
        )
        runner = _make_runner()
        entries = runner._parse_progress(tmp_path)
        assert len(entries) == 2

    def test_handles_missing_optional_fields(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text('{"status":"completed"}\n')
        runner = _make_runner()
        entries = runner._parse_progress(tmp_path)
        assert len(entries) == 1
        assert entries[0].step is None
        assert entries[0].message is None


class TestCheckRejection:
    def test_no_rejection(self):
        runner = _make_runner()
        entries = [
            ProgressEntry(step="Load", status="completed", message="OK"),
            ProgressEntry(step="Process", status="completed", message="Done"),
        ]
        assert runner._check_rejection(entries) is None

    def test_detects_rejection(self):
        runner = _make_runner()
        entries = [
            ProgressEntry(step="Load", status="completed", message="OK"),
            ProgressEntry(step="Validate", status="rejected", message="Missing atoms"),
        ]
        result = runner._check_rejection(entries)
        assert result == "Missing atoms"

    def test_empty_progress(self):
        runner = _make_runner()
        assert runner._check_rejection([]) is None

    def test_returns_last_rejection(self):
        """When multiple rejections exist, returns the most recent."""
        runner = _make_runner()
        entries = [
            ProgressEntry(step="A", status="rejected", message="First rejection"),
            ProgressEntry(step="B", status="rejected", message="Second rejection"),
        ]
        assert runner._check_rejection(entries) == "Second rejection"


class TestCollectFeatures:
    def test_empty_when_no_file(self, tmp_path: Path):
        runner = _make_runner()
        hook = _make_hook()
        features = runner._collect_features(tmp_path, hook)
        assert features == []

    def test_collects_list(self, tmp_path: Path):
        features_file = tmp_path / "features.json"
        features_file.write_text(
            json.dumps(
                [
                    {"score": 0.95, "pocket_id": "P1"},
                    {"score": 0.82, "pocket_id": "P2"},
                ]
            )
        )
        runner = _make_runner()
        features = runner._collect_features(tmp_path, _make_hook())
        assert len(features) == 2
        assert features[0]["score"] == 0.95

    def test_wraps_single_dict_in_list(self, tmp_path: Path):
        features_file = tmp_path / "features.json"
        features_file.write_text(json.dumps({"score": 0.95}))
        runner = _make_runner()
        features = runner._collect_features(tmp_path, _make_hook())
        assert len(features) == 1
        assert features[0]["score"] == 0.95

    def test_empty_list_returns_empty(self, tmp_path: Path):
        features_file = tmp_path / "features.json"
        features_file.write_text("[]")
        runner = _make_runner()
        features = runner._collect_features(tmp_path, _make_hook())
        assert features == []


class TestInputStaging:
    """Test that run() correctly stages inputs to the workspace."""

    @pytest.mark.asyncio
    async def test_stages_record_json(self, tmp_path: Path):
        """record.json is written to osa_in/."""
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "urn:osa:localhost:rec:123", "name": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        await runner.run(hook, inputs, workspace)

        record_file = workspace / "in" / "record.json"
        assert record_file.exists()
        data = json.loads(record_file.read_text())
        assert data["srn"] == "urn:osa:localhost:rec:123"

    @pytest.mark.asyncio
    async def test_stages_config_json(self, tmp_path: Path):
        """config.json is written when hook has config."""
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook(config={"r_min": 3.0})
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        await runner.run(hook, inputs, workspace)

        config_file = workspace / "in" / "config.json"
        assert config_file.exists()
        data = json.loads(config_file.read_text())
        assert data["r_min"] == 3.0


class TestContainerLifecycle:
    @pytest.mark.asyncio
    async def test_successful_hook_returns_passed(self, tmp_path: Path):
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.PASSED
        assert result.hook_name == "pocket_detect"
        assert result.duration_seconds > 0
        container.delete.assert_called_once_with(force=True)

    @pytest.mark.asyncio
    async def test_nonzero_exit_returns_failed(self, tmp_path: Path):
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 1}
        container.show.return_value = {"State": {"OOMKilled": False}}
        container.log.return_value = ["Error: something went wrong"]

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.FAILED
        assert "exit" in (result.error_message or "").lower()

    @pytest.mark.asyncio
    async def test_oom_killed_returns_failed(self, tmp_path: Path):
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 137}
        container.show.return_value = {"State": {"OOMKilled": True}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.FAILED
        assert "oom" in (result.error_message or "").lower()

    @pytest.mark.asyncio
    async def test_timeout_returns_failed(self, tmp_path: Path):
        import asyncio

        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container

        # Make container.wait hang forever
        async def hang():
            await asyncio.sleep(999)
            return {"StatusCode": 0}

        container.wait.side_effect = hang

        runner = OciHookRunner(docker=docker)
        hook = _make_hook(timeout=1)  # 1 second timeout
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.FAILED
        assert "timed out" in (result.error_message or "").lower()

    @pytest.mark.asyncio
    async def test_rejection_via_progress(self, tmp_path: Path):
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # Pre-create the output directory and rejection progress
        out_dir = workspace / "out"
        out_dir.mkdir(parents=True)
        (out_dir / "progress.jsonl").write_text(
            '{"step":"Validate","status":"rejected","message":"Missing atoms"}\n'
        )

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.REJECTED
        assert result.rejection_reason == "Missing atoms"

    @pytest.mark.asyncio
    async def test_features_collected_on_success(self, tmp_path: Path):
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # Pre-create features output
        out_dir = workspace / "out"
        out_dir.mkdir(parents=True)
        (out_dir / "features.json").write_text(json.dumps([{"score": 0.95}, {"score": 0.82}]))

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.PASSED
        assert len(result.features) == 2
        assert result.features[0]["score"] == 0.95


class TestContainerConfig:
    @pytest.mark.asyncio
    async def test_security_hardening(self, tmp_path: Path):
        """Container config includes security settings per Decision 9."""
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook(memory="4g", cpu="4.0")
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        await runner.run(hook, inputs, workspace)

        # Inspect the config passed to containers.create
        call_args = docker.containers.create.call_args
        config = call_args[0][0] if call_args[0] else call_args[1].get("config", {})

        host_config = config.get("HostConfig", {})
        assert host_config["NetworkMode"] == "none"
        assert host_config["ReadonlyRootfs"] is True
        assert host_config["CapDrop"] == ["ALL"]
        assert host_config["SecurityOpt"] == ["no-new-privileges"]
        assert host_config["PidsLimit"] == 256
        assert host_config["Memory"] == 4 * 1024 * 1024 * 1024
        assert host_config["NanoCpus"] == int(4.0 * 1e9)

    @pytest.mark.asyncio
    async def test_env_vars_set(self, tmp_path: Path):
        """Container gets OSA_IN and OSA_OUT env vars."""
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        await runner.run(hook, inputs, workspace)

        call_args = docker.containers.create.call_args
        config = call_args[0][0] if call_args[0] else call_args[1].get("config", {})

        assert "OSA_IN=/osa/in" in config["Env"]
        assert "OSA_OUT=/osa/out" in config["Env"]

    @pytest.mark.asyncio
    async def test_container_deleted_on_failure(self, tmp_path: Path):
        """Container is cleaned up even when hook fails."""
        import aiodocker

        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.side_effect = aiodocker.DockerError(500, {"message": "boom"})

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(record_json={"srn": "test"})

        workspace = tmp_path / "workspace"
        workspace.mkdir()

        result = await runner.run(hook, inputs, workspace)

        assert result.status == HookStatus.FAILED
        container.delete.assert_called_once_with(force=True)
