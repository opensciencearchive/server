"""Unit tests for K8sIngesterRunner — Job spec differences, source lifecycle."""

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.config import K8sConfig
from osa.domain.shared.error import ExternalServiceError
from osa.domain.shared.model.source import IngesterDefinition, IngesterLimits
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.port.ingester_runner import IngesterInputs
from osa.infrastructure.k8s.ingester_runner import K8sIngesterRunner

_CONV_SRN = ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_source(
    image: str = "ghcr.io/example/source:v1",
    digest: str = "sha256:abc123",
    timeout: int = 3600,
    memory: str = "4g",
    cpu: str = "2.0",
    config: dict[str, Any] | None = None,
) -> IngesterDefinition:
    return IngesterDefinition(
        image=image,
        digest=digest,
        config=config,
        limits=IngesterLimits(timeout_seconds=timeout, memory=memory, cpu=cpu),
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


def _make_s3_mock() -> AsyncMock:
    """Create an S3Client mock that returns sensible defaults."""
    s3 = AsyncMock()
    s3.get_object.return_value = b""
    s3.put_object.return_value = None
    s3.list_objects.return_value = []
    s3.head_object.return_value = False
    return s3


def _make_runner(config: K8sConfig | None = None) -> K8sIngesterRunner:
    api_client = MagicMock()
    s3 = _make_s3_mock()
    return K8sIngesterRunner(api_client=api_client, config=config or _make_config(), s3=s3)


# ---------------------------------------------------------------------------
# Job spec differences (T021)
# ---------------------------------------------------------------------------


class TestSourceJobSpec:
    def test_network_enabled(self):
        """Source Jobs have normal DNS policy (network access)."""
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
        )
        pod_spec = spec.spec.template.spec
        assert pod_spec.dns_policy is None or pod_spec.dns_policy != "None"

    def test_writable_rootfs(self):
        """Source containers do not have readOnlyRootFilesystem."""
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
        )
        sec = spec.spec.template.spec.containers[0].security_context
        assert sec is None or sec.read_only_root_filesystem is not True

    def test_higher_defaults(self):
        """Source Jobs use higher defaults (3600s, 4g)."""
        runner = _make_runner()
        source = _make_source(timeout=3600, memory="4g")
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
        )
        resources = spec.spec.template.spec.containers[0].resources
        assert resources.limits["memory"] == "4Gi"
        # activeDeadlineSeconds = scheduling_timeout + source timeout
        assert spec.spec.active_deadline_seconds == 120 + 3600

    def test_three_volume_mounts(self):
        """Source Jobs have input, output, and files mounts."""
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
        )
        mounts = spec.spec.template.spec.containers[0].volume_mounts
        mount_paths = {m.mount_path for m in mounts}
        assert "/osa/in" in mount_paths
        assert "/osa/out" in mount_paths
        assert "/osa/files" in mount_paths

    def test_files_mount_writable(self):
        """Source files mount is writable."""
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
        )
        mounts = spec.spec.template.spec.containers[0].volume_mounts
        files_mount = next(m for m in mounts if m.mount_path == "/osa/files")
        assert files_mount.read_only is not True

    def test_env_vars(self):
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
            inputs=IngesterInputs(convention_srn=_CONV_SRN, limit=100, offset=50),
        )
        env = spec.spec.template.spec.containers[0].env
        env_dict = {e.name: e.value for e in env}
        assert env_dict["OSA_IN"] == "/osa/in"
        assert env_dict["OSA_OUT"] == "/osa/out"
        assert env_dict["OSA_FILES"] == "/osa/files"
        assert env_dict["OSA_LIMIT"] == "100"
        assert env_dict["OSA_OFFSET"] == "50"

    def test_since_env_var(self):
        from datetime import datetime, UTC

        runner = _make_runner()
        source = _make_source()
        since = datetime(2026, 1, 1, tzinfo=UTC)
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
            inputs=IngesterInputs(convention_srn=_CONV_SRN, since=since),
        )
        env = spec.spec.template.spec.containers[0].env
        env_dict = {e.name: e.value for e in env}
        assert "OSA_SINCE" in env_dict

    def test_source_role_label(self):
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
        )
        labels = spec.spec.template.metadata.labels
        assert labels["osa.io/role"] == "source"

    def test_human_readable_name(self):
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:conv1@1.0.0"),
        )
        name = spec.metadata.name
        assert name.startswith("osa-source-")
        assert len(name) <= 63

    def test_convention_srn_in_labels(self):
        runner = _make_runner()
        source = _make_source()
        spec = runner._build_job_spec(
            source,
            work_dir=Path("/data/sources/localhost_conv1/staging/run1"),
            files_dir=Path("/data/sources/localhost_conv1/staging/run1/files"),
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:conv1@1.0.0"),
        )
        labels = spec.spec.template.metadata.labels
        assert labels["osa.io/convention"] == "localhost.conv.conv1.1.0.0"


# ---------------------------------------------------------------------------
# Source lifecycle (T022)
# ---------------------------------------------------------------------------


class TestSourceLifecycle:
    @pytest.mark.asyncio
    async def test_successful_run_with_records(self, tmp_path: Path):
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sIngesterRunner(api_client=MagicMock(), config=config, s3=_make_s3_mock())

        batch_api = AsyncMock()
        core_api = AsyncMock()

        # No existing jobs
        job_list = MagicMock()
        job_list.items = []
        batch_api.list_namespaced_job.return_value = job_list
        batch_api.create_namespaced_job.return_value = MagicMock()

        # Pod running
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

        source = _make_source()
        work_dir = tmp_path / "sources" / "localhost_conv1" / "staging" / "run1"
        files_dir = work_dir / "files"

        # Configure S3 mock to return records and session
        records_data = (
            b'{"id":"r1","metadata":{"title":"Test"}}\n{"id":"r2","metadata":{"title":"Test2"}}\n'
        )
        session_data = b'{"cursor":"abc"}'

        async def s3_get(key: str) -> bytes:
            if "records.jsonl" in key:
                return records_data
            if "session.json" in key:
                return session_data
            return b""

        runner._s3.get_object.side_effect = s3_get

        inputs = IngesterInputs(convention_srn=_CONV_SRN)
        result = await runner._run_job(
            batch_api,
            core_api,
            source,
            inputs,
            work_dir,
            files_dir,
        )

        assert len(result.records) == 2
        assert result.session == {"cursor": "abc"}
        assert result.files_dir == files_dir
        batch_api.delete_namespaced_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_timeout_raises_external_service_error(self, tmp_path: Path):
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sIngesterRunner(api_client=MagicMock(), config=config, s3=_make_s3_mock())

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

        source = _make_source()
        work_dir = tmp_path / "sources" / "localhost_conv1" / "staging" / "run1"
        work_dir.mkdir(parents=True)
        files_dir = work_dir / "files"
        files_dir.mkdir(parents=True)
        inputs = IngesterInputs(convention_srn=_CONV_SRN)

        with pytest.raises(ExternalServiceError, match="[Tt]imed out|[Dd]eadline"):
            await runner._run_job(
                batch_api,
                core_api,
                source,
                inputs,
                work_dir,
                files_dir,
            )

    @pytest.mark.asyncio
    async def test_oom_raises_external_service_error(self, tmp_path: Path):
        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sIngesterRunner(api_client=MagicMock(), config=config, s3=_make_s3_mock())

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

        failed_job = MagicMock()
        condition = MagicMock()
        condition.type = "Failed"
        condition.status = "True"
        condition.reason = "BackoffLimitExceeded"
        failed_job.status.conditions = [condition]
        failed_job.status.succeeded = None
        failed_job.status.failed = 1
        batch_api.read_namespaced_job.return_value = failed_job

        # OOMKilled pod
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

        core_api.list_namespaced_pod.side_effect = [pod_list, oom_pod_list]

        source = _make_source()
        work_dir = tmp_path / "sources" / "localhost_conv1" / "staging" / "run1"
        work_dir.mkdir(parents=True)
        files_dir = work_dir / "files"
        files_dir.mkdir(parents=True)
        inputs = IngesterInputs(convention_srn=_CONV_SRN)

        with pytest.raises(ExternalServiceError, match="[Oo]OM"):
            await runner._run_job(
                batch_api,
                core_api,
                source,
                inputs,
                work_dir,
                files_dir,
            )


# ---------------------------------------------------------------------------
# Identity threading from IngesterInputs
# ---------------------------------------------------------------------------


class TestConventionSrnFromInputs:
    """Verify run() threads convention_srn from inputs to Job labels."""

    @pytest.mark.asyncio
    async def test_run_uses_convention_srn_from_inputs(self, tmp_path: Path):
        from unittest.mock import patch

        config = _make_config(data_mount_path=str(tmp_path))
        runner = K8sIngesterRunner(api_client=MagicMock(), config=config, s3=_make_s3_mock())

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

        source = _make_source()
        work_dir = tmp_path / "sources" / "run1"
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True)
        files_dir = work_dir / "files"
        files_dir.mkdir(parents=True)

        inputs = IngesterInputs(
            convention_srn=ConventionSRN.parse("urn:osa:localhost:conv:my-conv@1.0.0")
        )

        with (
            patch("kubernetes_asyncio.client.BatchV1Api", return_value=batch_api),
            patch("kubernetes_asyncio.client.CoreV1Api", return_value=core_api),
        ):
            await runner.run(source, inputs, files_dir, work_dir)

        # Verify convention_srn from inputs ends up in the Job labels
        call_args = batch_api.create_namespaced_job.call_args
        spec = call_args[0][1]
        labels = spec.metadata.labels
        assert labels["osa.io/convention"] == "localhost.conv.my-conv.1.0.0"
