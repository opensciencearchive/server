"""Unit tests for K8sHookRunner — Job spec, scheduling, execution, orphans, cleanup."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.config import K8sConfig
from osa.domain.shared.error import InfrastructureError
from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    OciLimits,
    TableFeatureSpec,
)
from osa.domain.validation.model.hook_result import HookStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.infrastructure.k8s.runner import K8sHookRunner


def _make_hook(
    name: str = "validate_dna",
    timeout: int = 300,
    memory: str = "2g",
    cpu: str = "2.0",
    config: dict | None = None,
    image: str = "ghcr.io/example/hook:v1",
    digest: str = "sha256:abc123",
) -> HookDefinition:
    return HookDefinition(
        name=name,
        runtime=OciConfig(
            image=image,
            digest=digest,
            config=config or {},
            limits=OciLimits(timeout_seconds=timeout, memory=memory, cpu=cpu),
        ),
        feature=TableFeatureSpec(
            cardinality="many",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


def _make_config(**overrides) -> K8sConfig:
    defaults = {
        "namespace": "osa",
        "data_pvc_name": "osa-data-pvc",
        "data_mount_path": "/data",
        "job_ttl_seconds": 300,
    }
    defaults.update(overrides)
    return K8sConfig(**defaults)


def _make_runner(config: K8sConfig | None = None) -> K8sHookRunner:
    api_client = MagicMock()
    return K8sHookRunner(api_client=api_client, config=config or _make_config())


# ---------------------------------------------------------------------------
# Job spec generation (T014)
# ---------------------------------------------------------------------------


class TestJobSpecGeneration:
    def test_correct_image(self):
        runner = _make_runner()
        hook = _make_hook(image="ghcr.io/org/hook:v2", digest="sha256:def456")
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        container = spec.spec.template.spec.containers[0]
        assert container.image == "ghcr.io/org/hook:v2@sha256:def456"

    def test_security_context(self):
        runner = _make_runner()
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        pod_spec = spec.spec.template.spec
        container = pod_spec.containers[0]
        sec = container.security_context

        assert sec.read_only_root_filesystem is True
        assert sec.capabilities.drop == ["ALL"]
        assert sec.allow_privilege_escalation is False
        assert sec.run_as_user == 65534
        assert sec.run_as_group == 65534

        # Pod-level security context
        assert pod_spec.security_context.run_as_non_root is True

    def test_resource_limits(self):
        runner = _make_runner()
        hook = _make_hook(memory="4g", cpu="2.0")
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        resources = spec.spec.template.spec.containers[0].resources
        assert resources.limits["memory"] == "4g"
        assert resources.limits["cpu"] == "2.0"

    def test_volume_mounts(self):
        runner = _make_runner()
        hook = _make_hook()
        work_dir = Path("/data/depositions/localhost_abc/hooks/validate_dna")
        spec = runner._build_job_spec(hook, work_dir)

        volumes = spec.spec.template.spec.volumes
        pvc_vol = next(v for v in volumes if v.name == "data")
        assert pvc_vol.persistent_volume_claim.claim_name == "osa-data-pvc"

        tmp_vol = next(v for v in volumes if v.name == "tmp")
        assert tmp_vol.empty_dir is not None

        mounts = spec.spec.template.spec.containers[0].volume_mounts
        mount_paths = {m.mount_path for m in mounts}
        assert "/osa/in" in mount_paths
        assert "/osa/out" in mount_paths
        assert "/tmp" in mount_paths

    def test_env_vars(self):
        runner = _make_runner()
        hook = _make_hook(name="pocket_detect")
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/pocket_detect")
        )

        env = spec.spec.template.spec.containers[0].env
        env_dict = {e.name: e.value for e in env}
        assert env_dict["OSA_IN"] == "/osa/in"
        assert env_dict["OSA_OUT"] == "/osa/out"
        assert env_dict["OSA_HOOK_NAME"] == "pocket_detect"

    def test_backoff_limit_zero(self):
        runner = _make_runner()
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        assert spec.spec.backoff_limit == 0

    def test_active_deadline_seconds(self):
        runner = _make_runner()
        hook = _make_hook(timeout=300)
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        # scheduling_timeout (120) + hook timeout (300)
        assert spec.spec.active_deadline_seconds == 420

    def test_dns_policy_none(self):
        runner = _make_runner()
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        pod_spec = spec.spec.template.spec
        assert pod_spec.dns_policy == "None"
        assert pod_spec.dns_config.nameservers == []

    def test_labels(self):
        runner = _make_runner()
        hook = _make_hook(name="validate_dna")
        spec = runner._build_job_spec(
            hook,
            Path("/data/depositions/localhost_abc/hooks/validate_dna"),
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        labels = spec.spec.template.metadata.labels
        assert labels["osa.io/role"] == "hook"
        assert labels["osa.io/hook"] == "validate_dna"
        assert labels["osa.io/deposition"] == "urn:osa:localhost:dep:abc123"

    def test_human_readable_job_name(self):
        runner = _make_runner()
        hook = _make_hook(name="validate_dna")
        spec = runner._build_job_spec(
            hook,
            Path("/data/depositions/localhost_abc/hooks/validate_dna"),
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        name = spec.metadata.name
        assert name.startswith("osa-hook-")
        assert len(name) <= 63

    def test_empty_dir_at_tmp(self):
        runner = _make_runner()
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        volumes = spec.spec.template.spec.volumes
        tmp = next(v for v in volumes if v.name == "tmp")
        assert tmp.empty_dir.size_limit == "512Mi"

    def test_automount_service_account_false(self):
        runner = _make_runner()
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        pod_spec = spec.spec.template.spec
        assert pod_spec.automount_service_account_token is False

    def test_ttl_seconds_after_finished(self):
        runner = _make_runner(config=_make_config(job_ttl_seconds=600))
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        assert spec.spec.ttl_seconds_after_finished == 600

    def test_files_mount_when_files_dir_provided(self):
        runner = _make_runner()
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook,
            Path("/data/depositions/localhost_abc/hooks/validate_dna"),
            files_dir=Path("/data/depositions/localhost_abc/files"),
        )

        mounts = spec.spec.template.spec.containers[0].volume_mounts
        files_mount = next((m for m in mounts if m.mount_path == "/osa/in/files"), None)
        assert files_mount is not None
        assert files_mount.read_only is True

    def test_image_pull_secrets(self):
        runner = _make_runner(config=_make_config(image_pull_secrets=["ghcr-secret"]))
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        secrets = spec.spec.template.spec.image_pull_secrets
        assert len(secrets) == 1
        assert secrets[0].name == "ghcr-secret"

    def test_service_account(self):
        runner = _make_runner(config=_make_config(service_account="osa-runner"))
        hook = _make_hook()
        spec = runner._build_job_spec(
            hook, Path("/data/depositions/localhost_abc/hooks/validate_dna")
        )

        assert spec.spec.template.spec.service_account_name == "osa-runner"


# ---------------------------------------------------------------------------
# Path coordination (T015)
# ---------------------------------------------------------------------------


class TestPathCoordination:
    def test_relative_path_strips_prefix(self):
        runner = _make_runner(config=_make_config(data_mount_path="/data"))
        result = runner._relative_path(Path("/data/depositions/localhost_abc/hooks/validate"))
        assert result == "depositions/localhost_abc/hooks/validate"

    def test_relative_path_raises_outside_prefix(self):
        runner = _make_runner(config=_make_config(data_mount_path="/data"))
        with pytest.raises(ValueError, match="outside"):
            runner._relative_path(Path("/other/path"))

    def test_relative_path_handles_trailing_slash(self):
        runner = _make_runner(config=_make_config(data_mount_path="/data/"))
        result = runner._relative_path(Path("/data/depositions/test"))
        assert result == "depositions/test"


# ---------------------------------------------------------------------------
# Scheduling watch (T016)
# ---------------------------------------------------------------------------


class TestSchedulingWatch:
    @pytest.mark.asyncio
    async def test_pod_leaves_pending_quickly(self):
        runner = _make_runner()
        core_api = AsyncMock()

        # Pod transitions from Pending to Running
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        await runner._wait_for_scheduling(core_api, "test-job", "osa")

    @pytest.mark.asyncio
    async def test_pod_stuck_scheduling_timeout(self):
        runner = _make_runner()
        core_api = AsyncMock()

        # Pod stays in Pending
        pod = MagicMock()
        pod.status.phase = "Pending"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        with pytest.raises(InfrastructureError, match="scheduling"):
            await runner._wait_for_scheduling(
                core_api, "test-job", "osa", timeout_seconds=0.1, poll_interval=0.05
            )

    @pytest.mark.asyncio
    async def test_image_pull_backoff_fails_fast(self):
        runner = _make_runner()
        core_api = AsyncMock()

        pod = MagicMock()
        pod.status.phase = "Pending"
        container_status = MagicMock()
        container_status.state.waiting.reason = "ImagePullBackOff"
        container_status.state.waiting.message = "pull access denied"
        pod.status.container_statuses = [container_status]
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        with pytest.raises(InfrastructureError, match="[Ii]mage pull"):
            await runner._wait_for_scheduling(core_api, "test-job", "osa")

    @pytest.mark.asyncio
    async def test_err_image_pull_fails_fast(self):
        runner = _make_runner()
        core_api = AsyncMock()

        pod = MagicMock()
        pod.status.phase = "Pending"
        container_status = MagicMock()
        container_status.state.waiting.reason = "ErrImagePull"
        container_status.state.waiting.message = "not found"
        pod.status.container_statuses = [container_status]
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        with pytest.raises(InfrastructureError, match="[Ii]mage pull"):
            await runner._wait_for_scheduling(core_api, "test-job", "osa")

    @pytest.mark.asyncio
    async def test_pod_evicted(self):
        runner = _make_runner()
        core_api = AsyncMock()

        pod = MagicMock()
        pod.status.phase = "Failed"
        pod.status.reason = "Evicted"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        with pytest.raises(InfrastructureError, match="[Ee]vict"):
            await runner._wait_for_scheduling(core_api, "test-job", "osa")


# ---------------------------------------------------------------------------
# Execution watch + orphan handling + cleanup (T017)
# ---------------------------------------------------------------------------


class TestExecutionAndCleanup:
    @pytest.mark.asyncio
    async def test_successful_run(self, tmp_path: Path):
        """Full lifecycle: create → schedule → complete → parse → cleanup."""
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        # No existing jobs (orphan check)
        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list

        # Job creation succeeds
        batch_api.create_namespaced_job.return_value = MagicMock()

        # Pod leaves Pending
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        # Job completes successfully
        completed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Complete"
        condition.status = "True"
        completed_job.status.conditions = [condition]
        completed_job.status.succeeded = 1
        completed_job.status.failed = None
        batch_api.read_namespaced_job.return_value = completed_job

        # Create output directory with progress
        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "progress.jsonl").write_text(
            '{"step":"Check","status":"completed","message":"OK"}\n'
        )

        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )
        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.PASSED
        assert len(result.progress) == 1
        # Job should be cleaned up
        batch_api.delete_namespaced_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_deadline_exceeded(self, tmp_path: Path):
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list
        batch_api.create_namespaced_job.return_value = MagicMock()

        # Pod Running
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        # Job failed with DeadlineExceeded
        failed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Failed"
        condition.status = "True"
        condition.reason = "DeadlineExceeded"
        failed_job.status.conditions = [condition]
        failed_job.status.succeeded = None
        failed_job.status.failed = 1
        batch_api.read_namespaced_job.return_value = failed_job

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        work_dir.mkdir(parents=True)
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.FAILED
        assert (
            "timed out" in result.error_message.lower()
            or "deadline" in result.error_message.lower()
        )
        batch_api.delete_namespaced_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_oom_exit_137(self, tmp_path: Path):
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list
        batch_api.create_namespaced_job.return_value = MagicMock()

        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        # Job failed
        failed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Failed"
        condition.status = "True"
        condition.reason = "BackoffLimitExceeded"
        failed_job.status.conditions = [condition]
        failed_job.status.succeeded = None
        failed_job.status.failed = 1
        batch_api.read_namespaced_job.return_value = failed_job

        # Pod has OOMKilled container
        oom_pod = MagicMock()
        oom_pod.status.phase = "Failed"
        terminated = MagicMock()
        terminated.reason = "OOMKilled"
        terminated.exit_code = 137
        container_status = MagicMock()
        container_status.state.terminated = terminated
        oom_pod.status.container_statuses = [container_status]
        oom_pod_list = MagicMock()
        oom_pod_list.items = [oom_pod]
        # Second call to list_namespaced_pod returns the OOM pod
        core_api.list_namespaced_pod.side_effect = [pod_list, oom_pod_list]

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        work_dir.mkdir(parents=True)
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.FAILED
        assert "oom" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_nonzero_exit(self, tmp_path: Path):
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list
        batch_api.create_namespaced_job.return_value = MagicMock()

        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        failed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Failed"
        condition.status = "True"
        condition.reason = "BackoffLimitExceeded"
        failed_job.status.conditions = [condition]
        failed_job.status.succeeded = None
        failed_job.status.failed = 1
        batch_api.read_namespaced_job.return_value = failed_job

        # Pod with exit code 1
        exit_pod = MagicMock()
        exit_pod.status.phase = "Failed"
        terminated = MagicMock()
        terminated.reason = None
        terminated.exit_code = 1
        container_status = MagicMock()
        container_status.state.terminated = terminated
        exit_pod.status.container_statuses = [container_status]
        exit_pod_list = MagicMock()
        exit_pod_list.items = [exit_pod]
        core_api.list_namespaced_pod.side_effect = [pod_list, exit_pod_list]

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        work_dir.mkdir(parents=True)
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.FAILED
        assert "exit" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_orphan_running_job_attaches(self, tmp_path: Path):
        """Existing running Job → attach and wait for it."""
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        # Existing active job
        existing_job = MagicMock()
        existing_job.metadata.name = "osa-hook-existing"
        existing_job.status.succeeded = None
        existing_job.status.failed = None
        existing_job.status.active = 1
        job_list = MagicMock()
        job_list.items = [existing_job]
        batch_api.list_namespaced_job.return_value = job_list

        # Pod Running (scheduling check)
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        # Job completes
        completed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Complete"
        condition.status = "True"
        completed_job.status.conditions = [condition]
        completed_job.status.succeeded = 1
        completed_job.status.failed = None
        batch_api.read_namespaced_job.return_value = completed_job

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.PASSED
        # Should NOT have created a new job
        batch_api.create_namespaced_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_orphan_completed_job_reads_output(self, tmp_path: Path):
        """Existing completed Job → read its output."""
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        existing_job = MagicMock()
        existing_job.metadata.name = "osa-hook-existing"
        existing_job.status.succeeded = 1
        existing_job.status.failed = None
        existing_job.status.active = None
        job_list = MagicMock()
        job_list.items = [existing_job]
        batch_api.list_namespaced_job.return_value = job_list

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.PASSED
        batch_api.create_namespaced_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_orphan_failed_job_creates_new(self, tmp_path: Path):
        """Existing failed Job → create new one."""
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        existing_job = MagicMock()
        existing_job.metadata.name = "osa-hook-existing"
        existing_job.status.succeeded = None
        existing_job.status.failed = 1
        existing_job.status.active = None
        job_list = MagicMock()
        job_list.items = [existing_job]
        batch_api.list_namespaced_job.return_value = job_list

        batch_api.create_namespaced_job.return_value = MagicMock()

        # Pod Running
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        # New job completes
        completed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Complete"
        condition.status = "True"
        completed_job.status.conditions = [condition]
        completed_job.status.succeeded = 1
        completed_job.status.failed = None
        batch_api.read_namespaced_job.return_value = completed_job

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.PASSED
        batch_api.create_namespaced_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_404_ignored(self, tmp_path: Path):
        """404 on Job delete is ignored (already cleaned up)."""
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()

        class FakeNotFound(Exception):
            status = 404
            reason = "Not Found"

        batch_api.delete_namespaced_job.side_effect = FakeNotFound()

        # Should not raise
        await runner._cleanup_job(batch_api, "test-job", "osa")

    @pytest.mark.asyncio
    async def test_rejection_via_progress(self, tmp_path: Path):
        """Hook with rejected progress entry returns REJECTED."""
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list
        batch_api.create_namespaced_job.return_value = MagicMock()

        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        completed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Complete"
        condition.status = "True"
        completed_job.status.conditions = [condition]
        completed_job.status.succeeded = 1
        completed_job.status.failed = None
        batch_api.read_namespaced_job.return_value = completed_job

        hook = _make_hook()
        work_dir = tmp_path / "depositions" / "localhost_abc" / "hooks" / "validate_dna"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "progress.jsonl").write_text(
            '{"step":"Validate","status":"rejected","message":"Missing atoms"}\n'
        )
        inputs = HookInputs(
            record_json={"srn": "test"}, deposition_srn="urn:osa:localhost:dep:abc123"
        )

        result = await runner._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn="urn:osa:localhost:dep:abc123",
        )

        assert result.status == HookStatus.REJECTED
        assert result.rejection_reason == "Missing atoms"


# ---------------------------------------------------------------------------
# Identity threading from HookInputs
# ---------------------------------------------------------------------------


class TestDepositionSrnFromInputs:
    """Verify run() uses inputs.deposition_srn for Job labels, not path parsing."""

    @pytest.mark.asyncio
    async def test_run_uses_deposition_srn_from_inputs(self, tmp_path: Path):
        """The deposition SRN in Job labels comes from inputs, not the work_dir path."""
        from unittest.mock import patch

        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sHookRunner(api_client=MagicMock(), config=config)

        batch_api = AsyncMock()
        core_api = AsyncMock()

        # No existing jobs
        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list
        batch_api.create_namespaced_job.return_value = MagicMock()

        # Pod scheduled
        pod = MagicMock()
        pod.status.phase = "Running"
        pod.status.container_statuses = None
        pod_list = MagicMock()
        pod_list.items = [pod]
        core_api.list_namespaced_pod.return_value = pod_list

        # Job completes
        completed_job = MagicMock()
        completed_job.status.succeeded = 1
        completed_job.status.conditions = []
        completed_job.status.failed = None
        batch_api.read_namespaced_job.return_value = completed_job

        # Work dir does NOT follow depositions path convention — proves
        # we're not parsing the path to extract the SRN
        work_dir = tmp_path / "arbitrary" / "path"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        (output_dir / "progress.jsonl").write_text("")

        hook = _make_hook()
        inputs = HookInputs(
            record_json={"srn": "test"},
            deposition_srn="urn:osa:localhost:dep:my-real-srn",
        )

        with (
            patch("kubernetes_asyncio.client.BatchV1Api", return_value=batch_api),
            patch("kubernetes_asyncio.client.CoreV1Api", return_value=core_api),
        ):
            await runner.run(hook, inputs, work_dir)

        # Verify the Job was created with the SRN from inputs
        call_args = batch_api.create_namespaced_job.call_args
        spec = call_args[0][1]  # positional arg: (namespace, spec)
        labels = spec.metadata.labels
        assert labels["osa.io/deposition"] == "urn:osa:localhost:dep:my-real-srn"
