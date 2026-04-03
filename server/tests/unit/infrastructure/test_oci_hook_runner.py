"""Unit tests for OciHookRunner — container lifecycle, parsing, and bind-mount config."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.validation.model.hook_result import HookStatus, ProgressEntry
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.port.hook_runner import HookInputs
from osa.infrastructure.oci.runner import OciHookRunner
from osa.infrastructure.runner_utils import (
    detect_rejection,
    parse_memory,
    parse_progress_file,
    to_k8s_quantity,
)


def _make_hook(
    name: str = "pocket_detect",
    timeout: int = 300,
    memory: str = "2g",
    cpu: str = "2.0",
    config: dict | None = None,
) -> HookDefinition:
    return HookDefinition(
        name=name,
        runtime=OciConfig(
            image="ghcr.io/example/hook:v1",
            digest="sha256:abc123",
            config=config or {},
            limits=OciLimits(timeout_seconds=timeout, memory=memory, cpu=cpu),
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[
                ColumnDef(name="score", json_type="number", required=True),
            ],
        ),
    )


def _make_runner(docker: AsyncMock | None = None) -> OciHookRunner:
    return OciHookRunner(docker=docker or AsyncMock())


class TestParseMemory:
    def test_gigabytes(self):
        assert parse_memory("2g") == 2 * 1024 * 1024 * 1024

    def test_megabytes(self):
        assert parse_memory("512m") == 512 * 1024 * 1024

    def test_kilobytes(self):
        assert parse_memory("1024k") == 1024 * 1024

    def test_bare_bytes(self):
        assert parse_memory("1048576") == 1048576

    def test_fractional(self):
        assert parse_memory("1.5g") == int(1.5 * 1024 * 1024 * 1024)

    def test_case_insensitive(self):
        assert parse_memory("2G") == 2 * 1024 * 1024 * 1024

    def test_with_i_suffix(self):
        assert parse_memory("2gi") == 2 * 1024 * 1024 * 1024

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid memory format"):
            parse_memory("abc")


class TestToK8sQuantity:
    """Convert Docker-style memory strings to K8s resource quantities."""

    def test_gigabytes(self):
        assert to_k8s_quantity("2g") == "2Gi"

    def test_megabytes(self):
        assert to_k8s_quantity("512m") == "512Mi"

    def test_kilobytes(self):
        assert to_k8s_quantity("1024k") == "1024Ki"

    def test_bare_bytes(self):
        assert to_k8s_quantity("1048576") == "1048576"

    def test_fractional_gigabytes(self):
        assert to_k8s_quantity("1.5g") == "1536Mi"

    def test_fractional_megabytes(self):
        assert to_k8s_quantity("1.5m") == "1536Ki"

    def test_case_insensitive(self):
        assert to_k8s_quantity("2G") == "2Gi"

    def test_with_i_suffix(self):
        assert to_k8s_quantity("2gi") == "2Gi"

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="Invalid memory format"):
            to_k8s_quantity("abc")


class TestParseProgress:
    def test_empty_when_no_file(self, tmp_path: Path):
        entries = parse_progress_file(tmp_path)
        assert entries == []

    def test_parses_valid_jsonl(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text(
            '{"step":"Loading","status":"completed","message":"Done"}\n'
            '{"step":"Analyzing","status":"completed","message":"Finished"}\n'
        )
        entries = parse_progress_file(tmp_path)
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
        entries = parse_progress_file(tmp_path)
        assert len(entries) == 2

    def test_skips_blank_lines(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text(
            '{"step":"A","status":"completed"}\n\n{"step":"B","status":"completed"}\n'
        )
        entries = parse_progress_file(tmp_path)
        assert len(entries) == 2

    def test_handles_missing_optional_fields(self, tmp_path: Path):
        progress_file = tmp_path / "progress.jsonl"
        progress_file.write_text('{"status":"completed"}\n')
        entries = parse_progress_file(tmp_path)
        assert len(entries) == 1
        assert entries[0].step is None
        assert entries[0].message is None


class TestCheckRejection:
    def test_no_rejection(self):
        entries = [
            ProgressEntry(step="Load", status="completed", message="OK"),
            ProgressEntry(step="Process", status="completed", message="Done"),
        ]
        rejected, reason = detect_rejection(entries)
        assert not rejected
        assert reason is None

    def test_detects_rejection(self):
        entries = [
            ProgressEntry(step="Load", status="completed", message="OK"),
            ProgressEntry(step="Validate", status="rejected", message="Missing atoms"),
        ]
        rejected, reason = detect_rejection(entries)
        assert rejected
        assert reason == "Missing atoms"

    def test_empty_progress(self):
        rejected, reason = detect_rejection([])
        assert not rejected

    def test_returns_last_rejection(self):
        """When multiple rejections exist, returns the most recent."""
        entries = [
            ProgressEntry(step="A", status="rejected", message="First rejection"),
            ProgressEntry(step="B", status="rejected", message="Second rejection"),
        ]
        rejected, reason = detect_rejection(entries)
        assert reason == "Second rejection"


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
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = await runner.run(hook, inputs, output_dir)

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
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = await runner.run(hook, inputs, output_dir)

        assert result.status == HookStatus.FAILED
        assert "exit" in (result.error_message or "").lower()

    @pytest.mark.asyncio
    async def test_oom_killed_returns_oom(self, tmp_path: Path):
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 137}
        container.show.return_value = {"State": {"OOMKilled": True}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = await runner.run(hook, inputs, output_dir)

        assert result.status == HookStatus.OOM
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
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = await runner.run(hook, inputs, output_dir)

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

        work_dir = tmp_path / "hook_work"
        work_dir.mkdir()

        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        # Pre-create rejection progress in the output subdir (where the runner reads from)
        container_output = work_dir / "output"
        container_output.mkdir()
        (container_output / "progress.jsonl").write_text(
            '{"step":"Validate","status":"rejected","message":"Missing atoms"}\n'
        )

        result = await runner.run(hook, inputs, work_dir)

        assert result.status == HookStatus.REJECTED
        assert result.rejection_reason == "Missing atoms"


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
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        await runner.run(hook, inputs, output_dir)

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
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        await runner.run(hook, inputs, output_dir)

        call_args = docker.containers.create.call_args
        config = call_args[0][0] if call_args[0] else call_args[1].get("config", {})

        assert "OSA_IN=/osa/in" in config["Env"]
        assert "OSA_OUT=/osa/out" in config["Env"]
        assert "OSA_HOOK_NAME=pocket_detect" in config["Env"]

    @pytest.mark.asyncio
    async def test_nested_bind_mounts(self, tmp_path: Path):
        """Runner uses sibling input/ and output/ dirs under work_dir."""
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        files_dir = tmp_path / "files"
        files_dir.mkdir()
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
            files_dirs={"test": files_dir},
        )

        work_dir = tmp_path / "hook_work"
        work_dir.mkdir()

        await runner.run(hook, inputs, work_dir)

        call_args = docker.containers.create.call_args
        config = call_args[0][0] if call_args[0] else call_args[1].get("config", {})

        binds = config["HostConfig"]["Binds"]
        # Should have 3 binds: input:ro, output:rw, files/{id}:ro
        assert len(binds) == 3

        # input/ and output/ are sibling dirs under work_dir
        in_bind = [b for b in binds if b.endswith(":/osa/in:ro")][0]
        out_bind = [b for b in binds if b.endswith(":/osa/out:rw")][0]
        assert str(work_dir / "input") in in_bind
        assert str(work_dir / "output") in out_bind
        assert any(":/osa/files/test:ro" in b for b in binds)

    @pytest.mark.asyncio
    async def test_no_files_bind_when_no_files_dir(self, tmp_path: Path):
        """When files_dir is None, only staging and output mounts are created."""
        docker = AsyncMock()
        container = AsyncMock()
        docker.containers.create.return_value = container
        container.wait.return_value = {"StatusCode": 0}
        container.show.return_value = {"State": {"OOMKilled": False}}

        runner = OciHookRunner(docker=docker)
        hook = _make_hook()
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        await runner.run(hook, inputs, output_dir)

        call_args = docker.containers.create.call_args
        config = call_args[0][0] if call_args[0] else call_args[1].get("config", {})

        binds = config["HostConfig"]["Binds"]
        # staging + output + empty files base dir
        assert len(binds) == 3
        # No per-record file mounts
        assert not any(":/osa/files/" in b and ":ro" in b for b in binds if b.count("/") > 3)

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
        inputs = HookInputs(
            records=[HookRecord(id="test", metadata={})],
            run_id="test-run",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = await runner.run(hook, inputs, output_dir)

        assert result.status == HookStatus.FAILED
        container.delete.assert_called_once_with(force=True)
