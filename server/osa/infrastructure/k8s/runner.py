"""Kubernetes Job-based hook runner."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from osa.config import K8sConfig
from osa.domain.shared.error import InfrastructureError
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import DepositionSRN
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.infrastructure.k8s.errors import classify_api_error
from osa.infrastructure.k8s.naming import job_name, label_value
from osa.infrastructure.runner_utils import (
    detect_rejection,
    relative_path,
    to_k8s_quantity,
)

if TYPE_CHECKING:
    from kubernetes_asyncio.client import ApiClient, BatchV1Api, CoreV1Api, V1Job

    from osa.infrastructure.s3.client import S3Client

logger = logging.getLogger(__name__)

SCHEDULING_TIMEOUT = 120  # seconds to wait for pod to leave Pending


class K8sHookRunner(HookRunner):
    """Executes hooks as Kubernetes Jobs.

    Mirrors OciHookRunner's security posture using K8s-native equivalents:
    - Network isolation via dnsPolicy=None + NetworkPolicy labels
    - Read-only rootfs, dropped capabilities, non-root user
    - Resource limits via K8s resources.limits
    - Timeout via activeDeadlineSeconds
    """

    def __init__(self, api_client: ApiClient, config: K8sConfig, s3: S3Client) -> None:
        self._api_client = api_client
        self._config = config
        self._s3 = s3

    def _s3_prefix(self, work_dir: Path, subdir: str) -> str:
        """Convert a PVC path + subdir to an S3 key prefix."""
        return f"{relative_path(work_dir, self._config.data_mount_path)}/{subdir}"

    async def run(
        self,
        hook: HookDefinition,
        inputs: HookInputs,
        work_dir: Path,
    ) -> HookResult:
        try:
            from kubernetes_asyncio.client import BatchV1Api, CoreV1Api
        except ImportError:
            from osa.domain.shared.error import ConfigurationError

            raise ConfigurationError(
                "kubernetes-asyncio is required for K8s runner. Install with: pip install osa[k8s]"
            )

        batch_api = BatchV1Api(self._api_client)
        core_api = CoreV1Api(self._api_client)

        # Write input files to S3 (container reads them via PVC/S3 CSI)
        input_prefix = self._s3_prefix(work_dir, "input")
        await self._s3.put_object(f"{input_prefix}/record.json", json.dumps(inputs.record_json))
        if inputs.config or hook.runtime.config:
            config = {**hook.runtime.config, **(inputs.config or {})}
            await self._s3.put_object(f"{input_prefix}/config.json", json.dumps(config))

        return await self._run_job(
            batch_api,
            core_api,
            hook,
            inputs,
            work_dir,
            deposition_srn=inputs.deposition_srn,
        )

    async def _run_job(
        self,
        batch_api: BatchV1Api,
        core_api: CoreV1Api,
        hook: HookDefinition,
        inputs: HookInputs,
        work_dir: Path,
        *,
        deposition_srn: DepositionSRN,
    ) -> HookResult:
        """Core Job lifecycle: check orphans → create → schedule → execute → parse → cleanup."""
        namespace = self._config.namespace
        start_time = time.monotonic()

        # Check for existing Jobs (orphan handling)
        job_name_to_watch = None

        try:
            existing = await self._check_existing_job(
                batch_api, namespace, hook.name, deposition_srn
            )

            if existing == "succeeded":
                # Read output from completed Job
                return await self._parse_hook_result(hook, work_dir, start_time)

            if existing and existing.startswith("active:"):
                # Attach to running Job
                job_name_to_watch = existing.split(":", 1)[1]
            else:
                # Create new Job (no existing or failed)
                spec = self._build_job_spec(
                    hook,
                    work_dir,
                    deposition_srn=deposition_srn,
                    files_dir=inputs.files_dir,
                )
                job_name_to_watch = spec.metadata.name

                await batch_api.create_namespaced_job(namespace, spec)
                logger.info(
                    "Created K8s Job",
                    extra={
                        "job_name": job_name_to_watch,
                        "namespace": namespace,
                        "image": f"{hook.runtime.image}@{hook.runtime.digest}",
                        "hook_name": hook.name,
                        "deposition_srn": deposition_srn,
                    },
                )

            # Phase 1: Wait for scheduling
            await self._wait_for_scheduling(core_api, job_name_to_watch, namespace)

            # Phase 2: Wait for completion
            result = await self._wait_for_completion(
                batch_api,
                job_name_to_watch,
                namespace,
                timeout_seconds=hook.runtime.limits.timeout_seconds + 30,
            )

            if result == "succeeded":
                return await self._parse_hook_result(hook, work_dir, start_time)

            # Job failed — determine why
            return await self._diagnose_failure(
                core_api, job_name_to_watch, namespace, hook, start_time, result
            )

        finally:
            if job_name_to_watch:
                await self._cleanup_job(batch_api, job_name_to_watch, namespace)

    async def _parse_hook_result(
        self, hook: HookDefinition, work_dir: Path, start_time: float
    ) -> HookResult:
        """Parse output from a completed Job (reads from S3)."""
        from osa.infrastructure.runner_utils import parse_progress_from_s3

        output_prefix = self._s3_prefix(work_dir, "output")
        progress = await parse_progress_from_s3(self._s3, output_prefix)
        duration = time.monotonic() - start_time

        rejected, reason = detect_rejection(progress)
        if rejected:
            return HookResult(
                hook_name=hook.name,
                status=HookStatus.REJECTED,
                rejection_reason=reason,
                progress=progress,
                duration_seconds=duration,
            )

        return HookResult(
            hook_name=hook.name,
            status=HookStatus.PASSED,
            progress=progress,
            duration_seconds=duration,
        )

    async def _check_existing_job(
        self,
        batch_api: BatchV1Api,
        namespace: str,
        hook_name: str,
        deposition_srn: DepositionSRN,
    ) -> str | None:
        """Check for existing Jobs with matching labels.

        Returns:
            "succeeded" if a completed Job exists
            "active:{job_name}" if a running Job exists
            None if no Job or only failed Jobs exist
        """
        label_selector = f"osa.io/hook={hook_name},osa.io/deposition={label_value(deposition_srn)}"
        try:
            job_list = await batch_api.list_namespaced_job(namespace, label_selector=label_selector)
        except Exception as exc:
            raise classify_api_error(exc) from exc

        for job in job_list.items:
            if job.status.succeeded:
                return "succeeded"
            if job.status.active:
                return f"active:{job.metadata.name}"

        return None

    def _build_job_spec(
        self,
        hook: HookDefinition,
        work_dir: Path,
        *,
        deposition_srn: DepositionSRN,
        files_dir: Path | None = None,
    ) -> V1Job:
        """Build a K8s Job manifest for a hook execution."""
        from kubernetes_asyncio.client import (
            V1Capabilities,
            V1Container,
            V1EmptyDirVolumeSource,
            V1EnvVar,
            V1Job,
            V1JobSpec,
            V1LocalObjectReference,
            V1ObjectMeta,
            V1PersistentVolumeClaimVolumeSource,
            V1PodDNSConfig,
            V1PodSecurityContext,
            V1PodSpec,
            V1SeccompProfile,
            V1PodTemplateSpec,
            V1ResourceRequirements,
            V1SecurityContext,
            V1Volume,
            V1VolumeMount,
        )

        name = job_name("hook", hook.name, str(deposition_srn))
        relative_work = self._relative_path(work_dir)
        input_subpath = f"{relative_work}/input"
        output_subpath = f"{relative_work}/output"

        labels = {
            "osa.io/role": "hook",
            "osa.io/hook": hook.name,
            "osa.io/deposition": label_value(deposition_srn),
        }

        mounts = [
            V1VolumeMount(
                name="data", mount_path="/osa/in", sub_path=input_subpath, read_only=True
            ),
            V1VolumeMount(
                name="data", mount_path="/osa/out", sub_path=output_subpath, read_only=False
            ),
            V1VolumeMount(name="tmp", mount_path="/tmp"),
        ]

        if files_dir:
            relative_files = self._relative_path(files_dir)
            mounts.append(
                V1VolumeMount(
                    name="data", mount_path="/osa/in/files", sub_path=relative_files, read_only=True
                )
            )

        volumes = [
            V1Volume(
                name="data",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=self._config.data_pvc_name
                ),
            ),
            V1Volume(name="tmp", empty_dir=V1EmptyDirVolumeSource(size_limit="512Mi")),
        ]

        container = V1Container(
            name="hook",
            image=f"{hook.runtime.image}@{hook.runtime.digest}",
            env=[
                V1EnvVar(name="OSA_IN", value="/osa/in"),
                V1EnvVar(name="OSA_OUT", value="/osa/out"),
                V1EnvVar(name="OSA_HOOK_NAME", value=hook.name),
            ],
            resources=V1ResourceRequirements(
                limits={
                    "memory": to_k8s_quantity(hook.runtime.limits.memory),
                    "cpu": hook.runtime.limits.cpu,
                },
            ),
            security_context=V1SecurityContext(
                read_only_root_filesystem=True,
                capabilities=V1Capabilities(drop=["ALL"]),
                allow_privilege_escalation=False,
                run_as_user=65534,
                run_as_group=65534,
                seccomp_profile=V1SeccompProfile(type="RuntimeDefault"),
            ),
            volume_mounts=mounts,
        )

        pod_spec = V1PodSpec(
            restart_policy="Never",
            automount_service_account_token=False,
            security_context=V1PodSecurityContext(
                run_as_non_root=True,
                fs_group=65534,
                seccomp_profile=V1SeccompProfile(type="RuntimeDefault"),
            ),
            dns_policy="None",
            dns_config=V1PodDNSConfig(nameservers=["127.0.0.1"]),
            containers=[container],
            volumes=volumes,
            image_pull_secrets=[
                V1LocalObjectReference(name=s) for s in self._config.image_pull_secrets
            ]
            or None,
            service_account_name=self._config.service_account,
        )

        return V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=V1ObjectMeta(name=name, namespace=self._config.namespace, labels=labels),
            spec=V1JobSpec(
                backoff_limit=0,
                active_deadline_seconds=SCHEDULING_TIMEOUT + hook.runtime.limits.timeout_seconds,
                ttl_seconds_after_finished=self._config.job_ttl_seconds,
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels=labels),
                    spec=pod_spec,
                ),
            ),
        )

    def _relative_path(self, path: Path) -> str:
        """Strip the data mount prefix to get a PVC-relative subpath."""
        return relative_path(path, self._config.data_mount_path)

    async def _wait_for_scheduling(
        self,
        core_api: CoreV1Api,
        job_name: str,
        namespace: str,
        *,
        timeout_seconds: float = SCHEDULING_TIMEOUT,
        poll_interval: float = 2.0,
    ) -> None:
        """Wait for the Job's pod to leave Pending (Phase 1)."""
        deadline = time.monotonic() + timeout_seconds
        label_selector = f"job-name={job_name}"

        while time.monotonic() < deadline:
            try:
                pod_list = await core_api.list_namespaced_pod(
                    namespace, label_selector=label_selector
                )
            except Exception as exc:
                raise classify_api_error(exc) from exc

            for pod in pod_list.items:
                phase = pod.status.phase

                # Check for eviction
                if phase == "Failed":
                    reason = getattr(pod.status, "reason", None) or "Unknown"
                    raise InfrastructureError(f"Pod evicted or failed during scheduling: {reason}")

                # Check for image pull errors
                if phase == "Pending" and pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        waiting = getattr(cs.state, "waiting", None)
                        if waiting and waiting.reason in ("ImagePullBackOff", "ErrImagePull"):
                            message = getattr(waiting, "message", "")
                            raise InfrastructureError(
                                f"Image pull failed: {waiting.reason}: {message}"
                            )

                if phase in ("Running", "Succeeded", "Failed"):
                    return  # Pod scheduled

            await asyncio.sleep(poll_interval)

        raise InfrastructureError(
            f"Pod scheduling timeout after {timeout_seconds}s for Job {job_name}"
        )

    async def _wait_for_completion(
        self,
        batch_api: BatchV1Api,
        job_name: str,
        namespace: str,
        *,
        timeout_seconds: float = 330,
        poll_interval: float = 5.0,
    ) -> str:
        """Wait for Job to complete (Phase 2). Returns 'succeeded' or 'failed'."""
        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            try:
                job = await batch_api.read_namespaced_job(job_name, namespace)
            except Exception as exc:
                raise classify_api_error(exc) from exc

            if job.status.succeeded:
                return "succeeded"

            if job.status.conditions:
                for condition in job.status.conditions:
                    if condition.type == "Failed" and condition.status == "True":
                        return f"failed:{getattr(condition, 'reason', 'Unknown')}"
                    if condition.type == "Complete" and condition.status == "True":
                        return "succeeded"

            if job.status.failed:
                return "failed:BackoffLimitExceeded"

            await asyncio.sleep(poll_interval)

        # Timed out — poll once more
        try:
            job = await batch_api.read_namespaced_job(job_name, namespace)
            if job.status.succeeded:
                return "succeeded"
        except Exception:
            pass

        return "failed:WatchTimeout"

    async def _diagnose_failure(
        self,
        core_api: CoreV1Api,
        job_name: str,
        namespace: str,
        hook: HookDefinition,
        start_time: float,
        failure_info: str,
    ) -> HookResult:
        """Determine failure reason from pod status."""
        duration = time.monotonic() - start_time

        # Check if DeadlineExceeded
        if "DeadlineExceeded" in failure_info:
            return HookResult(
                hook_name=hook.name,
                status=HookStatus.FAILED,
                error_message="Hook timed out (deadline exceeded)",
                duration_seconds=duration,
            )

        # Check pod for OOM or exit code
        try:
            label_selector = f"job-name={job_name}"
            pod_list = await core_api.list_namespaced_pod(namespace, label_selector=label_selector)
            for pod in pod_list.items:
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        terminated = getattr(cs.state, "terminated", None)
                        if terminated:
                            if getattr(terminated, "reason", None) == "OOMKilled":
                                return HookResult(
                                    hook_name=hook.name,
                                    status=HookStatus.FAILED,
                                    error_message="Hook killed by OOM",
                                    duration_seconds=duration,
                                )
                            exit_code = getattr(terminated, "exit_code", -1)
                            if exit_code != 0:
                                return HookResult(
                                    hook_name=hook.name,
                                    status=HookStatus.FAILED,
                                    error_message=f"Hook exited with code {exit_code}",
                                    duration_seconds=duration,
                                )
        except Exception:
            pass

        return HookResult(
            hook_name=hook.name,
            status=HookStatus.FAILED,
            error_message=f"Hook failed: {failure_info}",
            duration_seconds=duration,
        )

    async def _cleanup_job(
        self,
        batch_api: BatchV1Api,
        job_name: str,
        namespace: str,
    ) -> None:
        """Delete a Job and its pods. Ignores 404 (already cleaned up)."""
        try:
            await batch_api.delete_namespaced_job(
                job_name,
                namespace,
                propagation_policy="Background",
            )
            logger.info("Cleaned up K8s Job", extra={"job_name": job_name})
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return  # Already gone
            logger.warning(
                "Failed to clean up K8s Job",
                extra={"job_name": job_name, "error": str(exc)},
            )
