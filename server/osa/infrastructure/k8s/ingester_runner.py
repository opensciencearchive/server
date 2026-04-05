"""Kubernetes Job-based ingester runner."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from osa.config import K8sConfig
from osa.domain.shared.error import (
    InfrastructureError,
    OOMError,
    PermanentError,
    TransientError,
)
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterOutput, IngesterRunner
from osa.infrastructure.k8s.errors import classify_api_error
from osa.infrastructure.k8s.naming import job_name, label_value, sanitize_label
from osa.infrastructure.runner_utils import (
    relative_path,
    to_k8s_quantity,
)

if TYPE_CHECKING:
    from kubernetes_asyncio.client import ApiClient, V1Job

    from osa.infrastructure.s3.client import S3Client

logger = logging.getLogger(__name__)

SCHEDULING_TIMEOUT = 120


class K8sIngesterRunner(IngesterRunner):
    """Executes sources as Kubernetes Jobs.

    Key differences from K8sHookRunner:
    - Network enabled (normal DNS, no dnsPolicy override)
    - Writable rootfs (no readOnlyRootFilesystem)
    - Three volume mounts: input (ro), output (rw), files (rw)
    - Higher resource defaults (3600s, 4g)
    - Source-specific env vars (OSA_FILES, OSA_SINCE, etc.)
    """

    def __init__(self, api_client: ApiClient, config: K8sConfig, s3: S3Client) -> None:
        from kubernetes_asyncio.client import BatchV1Api, CoreV1Api

        self._batch_api = BatchV1Api(api_client)
        self._core_api = CoreV1Api(api_client)
        self._config = config
        self._s3 = s3

    def _s3_prefix(self, work_dir: Path, subdir: str) -> str:
        """Convert a PVC path + subdir to an S3 key prefix."""
        return f"{relative_path(work_dir, self._config.data_mount_path)}/{subdir}"

    async def capture_logs(self, run_id: str) -> str:
        """Capture recent pod logs for an ingester Job identified by run_id."""
        namespace = self._config.namespace
        label_selector = f"osa.io/role=ingester,osa.io/ingest-run-id={run_id}"
        try:
            pod_list = await self._core_api.list_namespaced_pod(
                namespace, label_selector=label_selector
            )
            for pod in pod_list.items:
                log_str = await self._core_api.read_namespaced_pod_log(
                    pod.metadata.name, namespace, tail_lines=10
                )
                return log_str.strip() if log_str else ""
        except Exception:
            return ""
        return ""

    async def run(
        self,
        ingester: IngesterDefinition,
        inputs: IngesterInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> IngesterOutput:
        # Write input files to S3 (container reads them via PVC/S3 CSI)
        input_prefix = self._s3_prefix(work_dir, "input")

        if inputs.config or ingester.config:
            config = {**(ingester.config or {}), **(inputs.config or {})}
            await self._s3.put_object(f"{input_prefix}/config.json", json.dumps(config))

        if inputs.session:
            await self._s3.put_object(f"{input_prefix}/session.json", json.dumps(inputs.session))

        return await self._run_job(ingester, inputs, work_dir, files_dir)

    async def _run_job(
        self,
        ingester: IngesterDefinition,
        inputs: IngesterInputs,
        work_dir: Path,
        files_dir: Path,
    ) -> IngesterOutput:
        """Core Job lifecycle for ingester execution."""
        namespace = self._config.namespace
        job_name_to_watch = None

        try:
            # Check for existing Jobs
            existing = await self._check_existing_job(
                namespace, inputs.convention_srn, ingester.digest
            )

            if existing == "succeeded":
                return await self._parse_source_output(work_dir, files_dir)

            if existing and existing.startswith("active:"):
                job_name_to_watch = existing.split(":", 1)[1]
            else:
                # Clear stale output from previous failed runs
                output_prefix = self._s3_prefix(work_dir, "output")
                await self._s3.delete_objects(output_prefix)

                spec = self._build_job_spec(
                    ingester,
                    work_dir=work_dir,
                    files_dir=files_dir,
                    inputs=inputs,
                    convention_srn=inputs.convention_srn,
                )
                job_name_to_watch = spec.metadata.name

                await self._batch_api.create_namespaced_job(namespace, spec)
                logger.info(
                    "Created K8s ingester Job",
                    extra={
                        "job_name": job_name_to_watch,
                        "namespace": namespace,
                        "image": f"{ingester.image}@{ingester.digest}",
                    },
                )

            # Phase 1: Scheduling
            await self._wait_for_scheduling(job_name_to_watch, namespace)

            # Phase 2: Completion (raises on failure)
            await self._wait_for_completion(
                job_name_to_watch,
                namespace,
                timeout_seconds=ingester.limits.timeout_seconds + 30,
            )

            output = await self._parse_source_output(work_dir, files_dir)
            logger.info(
                "Source completed",
                extra={
                    "job_name": job_name_to_watch,
                    "record_count": len(output.records),
                    "has_session": output.session is not None,
                },
            )
            return output

        finally:
            if job_name_to_watch:
                await self._cleanup_job(job_name_to_watch, namespace)

    async def _parse_source_output(self, work_dir: Path, files_dir: Path) -> IngesterOutput:
        from osa.infrastructure.runner_utils import (
            parse_records_from_s3,
            parse_session_from_s3,
        )

        output_prefix = self._s3_prefix(work_dir, "output")
        records = await parse_records_from_s3(self._s3, output_prefix)
        session = await parse_session_from_s3(self._s3, output_prefix)
        return IngesterOutput(records=records, session=session, files_dir=files_dir)

    async def _check_existing_job(
        self,
        namespace: str,
        convention_srn: ConventionSRN | None,
        digest: str = "",
    ) -> str | None:
        label_parts = ["osa.io/role=ingester"]
        if convention_srn is not None:
            label_parts.append(f"osa.io/convention={label_value(convention_srn)}")
        if digest:
            label_parts.append(f"osa.io/digest={sanitize_label(digest)}")
        label_selector = ",".join(label_parts)

        try:
            job_list = await self._batch_api.list_namespaced_job(
                namespace, label_selector=label_selector
            )
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
        ingester: IngesterDefinition,
        *,
        work_dir: Path,
        files_dir: Path,
        inputs: IngesterInputs | None = None,
        convention_srn: ConventionSRN | None = None,
    ) -> V1Job:
        from kubernetes_asyncio.client import (
            V1Capabilities,
            V1Container,
            V1EnvVar,
            V1Job,
            V1JobSpec,
            V1LocalObjectReference,
            V1ObjectMeta,
            V1PersistentVolumeClaimVolumeSource,
            V1PodSecurityContext,
            V1PodSpec,
            V1SeccompProfile,
            V1PodTemplateSpec,
            V1ResourceRequirements,
            V1SecurityContext,
            V1Volume,
            V1VolumeMount,
        )

        name = job_name("ingester", "ing", str(convention_srn) if convention_srn else "unknown")
        relative_work = self._relative_path(work_dir)
        input_subpath = f"{relative_work}/input"
        output_subpath = f"{relative_work}/output"
        relative_files = self._relative_path(files_dir)

        labels: dict[str, str] = {
            "osa.io/role": "ingester",
            "osa.io/digest": sanitize_label(ingester.digest),
        }
        if convention_srn is not None:
            labels["osa.io/convention"] = label_value(convention_srn)
        if inputs and inputs.ingest_run_id:
            labels["osa.io/ingest-run-id"] = inputs.ingest_run_id
            labels["osa.io/ingest-run-batch"] = str(inputs.batch_index)

        env = [
            V1EnvVar(name="OSA_IN", value="/osa/in"),
            V1EnvVar(name="OSA_OUT", value="/osa/out"),
            V1EnvVar(name="OSA_FILES", value="/osa/files"),
        ]
        if inputs:
            if inputs.since is not None:
                env.append(V1EnvVar(name="OSA_SINCE", value=inputs.since.isoformat()))
            if inputs.limit is not None:
                env.append(V1EnvVar(name="OSA_LIMIT", value=str(inputs.limit)))
            if inputs.offset:
                env.append(V1EnvVar(name="OSA_OFFSET", value=str(inputs.offset)))

        mounts = [
            V1VolumeMount(
                name="data", mount_path="/osa/in", sub_path=input_subpath, read_only=True
            ),
            V1VolumeMount(name="data", mount_path="/osa/out", sub_path=output_subpath),
            V1VolumeMount(name="data", mount_path="/osa/files", sub_path=relative_files),
        ]

        volumes = [
            V1Volume(
                name="data",
                persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                    claim_name=self._config.data_pvc_name
                ),
            ),
        ]

        container = V1Container(
            name="ingester",
            image=f"{ingester.image}@{ingester.digest}",
            env=env,
            resources=V1ResourceRequirements(
                limits={
                    "memory": to_k8s_quantity(ingester.limits.memory),
                    "cpu": ingester.limits.cpu,
                },
            ),
            security_context=V1SecurityContext(
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
                active_deadline_seconds=SCHEDULING_TIMEOUT + ingester.limits.timeout_seconds,
                ttl_seconds_after_finished=self._config.job_ttl_seconds,
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels=labels),
                    spec=pod_spec,
                ),
            ),
        )

    def _relative_path(self, path: Path) -> str:
        return relative_path(path, self._config.data_mount_path)

    async def _wait_for_scheduling(
        self,
        job_name: str,
        namespace: str,
        *,
        timeout_seconds: float = SCHEDULING_TIMEOUT,
        poll_interval: float = 2.0,
    ) -> None:
        deadline = time.monotonic() + timeout_seconds
        label_selector = f"job-name={job_name}"

        while time.monotonic() < deadline:
            try:
                pod_list = await self._core_api.list_namespaced_pod(
                    namespace, label_selector=label_selector
                )
            except Exception as exc:
                raise classify_api_error(exc) from exc

            for pod in pod_list.items:
                phase = pod.status.phase
                if phase == "Failed":
                    reason = getattr(pod.status, "reason", None) or "Unknown"
                    raise TransientError(f"Pod failed during scheduling: {reason}")

                if phase == "Pending" and pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        waiting = getattr(cs.state, "waiting", None)
                        if waiting and waiting.reason in ("ImagePullBackOff", "ErrImagePull"):
                            raise PermanentError(
                                f"Image pull failed: {waiting.reason}: {getattr(waiting, 'message', '')}"
                            )

                if phase in ("Running", "Succeeded", "Failed"):
                    return

            await asyncio.sleep(poll_interval)

        raise TransientError(f"Pod scheduling timeout after {timeout_seconds}s for Job {job_name}")

    async def _wait_for_completion(
        self,
        job_name: str,
        namespace: str,
        *,
        timeout_seconds: float = 3630,
        poll_interval: float = 5.0,
    ) -> None:
        """Wait for Job to complete. Returns on success, raises on failure."""
        deadline = time.monotonic() + timeout_seconds

        while time.monotonic() < deadline:
            try:
                job = await self._batch_api.read_namespaced_job(job_name, namespace)
            except Exception as exc:
                raise classify_api_error(exc) from exc

            if job.status.succeeded:
                return
            if job.status.conditions:
                for condition in job.status.conditions:
                    if condition.type == "Failed" and condition.status == "True":
                        failure_reason = getattr(condition, "reason", "Unknown")
                        raise await self._diagnose_failure(job_name, namespace, failure_reason)
                    if condition.type == "Complete" and condition.status == "True":
                        return
            if job.status.failed:
                raise await self._diagnose_failure(job_name, namespace, "BackoffLimitExceeded")

            await asyncio.sleep(poll_interval)

        # Timed out — poll once more to catch last-millisecond completions
        try:
            job = await self._batch_api.read_namespaced_job(job_name, namespace)
            if job.status.succeeded:
                return
        except Exception:
            pass

        raise TransientError(f"Watch timeout waiting for ingester Job {job_name} completion")

    async def _diagnose_failure(
        self,
        job_name: str,
        namespace: str,
        failure_info: str,
    ) -> InfrastructureError:
        """Inspect pod status and return the appropriate exception."""
        if "DeadlineExceeded" in failure_info:
            return TransientError("Ingester timed out (deadline exceeded)")

        try:
            label_selector = f"job-name={job_name}"
            pod_list = await self._core_api.list_namespaced_pod(
                namespace, label_selector=label_selector
            )
            for pod in pod_list.items:
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        terminated = getattr(cs.state, "terminated", None)
                        if terminated:
                            if getattr(terminated, "reason", None) == "OOMKilled":
                                return OOMError("Source killed by OOM")
                            exit_code = getattr(terminated, "exit_code", -1)
                            if exit_code != 0:
                                # Transient: ingester non-zero exit is often an upstream
                                # API failure (500, rate limit), not a code bug.
                                # Contrast with hooks where non-zero = PermanentError.
                                return TransientError(f"Source exited with code {exit_code}")
        except Exception:
            pass

        return PermanentError(f"Source failed: {failure_info}")

    async def _cleanup_job(self, job_name: str, namespace: str) -> None:
        try:
            await self._batch_api.delete_namespaced_job(
                job_name,
                namespace,
                propagation_policy="Background",
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return
            logger.warning("Failed to clean up K8s ingester Job", extra={"job_name": job_name})
