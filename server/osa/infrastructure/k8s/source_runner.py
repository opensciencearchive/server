"""Kubernetes Job-based source runner."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING

from osa.config import K8sConfig
from osa.domain.shared.error import ExternalServiceError, InfrastructureError
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.shared.model.srn import ConventionSRN
from osa.domain.source.port.source_runner import SourceInputs, SourceOutput, SourceRunner
from osa.infrastructure.k8s.errors import classify_api_error
from osa.infrastructure.k8s.naming import job_name, label_value, sanitize_label
from osa.infrastructure.runner_utils import parse_records_file, parse_session_file

if TYPE_CHECKING:
    from kubernetes_asyncio.client import ApiClient, BatchV1Api, CoreV1Api, V1Job

logger = logging.getLogger(__name__)

SCHEDULING_TIMEOUT = 120


class K8sSourceRunner(SourceRunner):
    """Executes sources as Kubernetes Jobs.

    Key differences from K8sHookRunner:
    - Network enabled (normal DNS, no dnsPolicy override)
    - Writable rootfs (no readOnlyRootFilesystem)
    - Three volume mounts: input (ro), output (rw), files (rw)
    - Higher resource defaults (3600s, 4g)
    - Source-specific env vars (OSA_FILES, OSA_SINCE, etc.)
    - Errors raise ExternalServiceError (not returned as result values)
    """

    def __init__(self, api_client: ApiClient, config: K8sConfig) -> None:
        self._api_client = api_client
        self._config = config

    async def run(
        self,
        source: SourceDefinition,
        inputs: SourceInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> SourceOutput:
        try:
            from kubernetes_asyncio.client import BatchV1Api, CoreV1Api
        except ImportError:
            from osa.domain.shared.error import ConfigurationError

            raise ConfigurationError(
                "kubernetes-asyncio is required for K8s runner. Install with: pip install osa[k8s]"
            )

        batch_api = BatchV1Api(self._api_client)
        core_api = CoreV1Api(self._api_client)

        # Write input files
        input_dir = work_dir / "input"
        input_dir.mkdir(parents=True, exist_ok=True)
        output_dir = work_dir / "output"
        output_dir.mkdir(parents=True, exist_ok=True)
        files_dir.mkdir(parents=True, exist_ok=True)

        if inputs.config or source.config:
            config = {**(source.config or {}), **(inputs.config or {})}
            (input_dir / "config.json").write_text(json.dumps(config))

        if inputs.session:
            (input_dir / "session.json").write_text(json.dumps(inputs.session))

        return await self._run_job(
            batch_api,
            core_api,
            source,
            inputs,
            work_dir,
            files_dir,
            convention_srn=inputs.convention_srn,
        )

    async def _run_job(
        self,
        batch_api: BatchV1Api,
        core_api: CoreV1Api,
        source: SourceDefinition,
        inputs: SourceInputs,
        work_dir: Path,
        files_dir: Path,
        *,
        convention_srn: ConventionSRN | None = None,
    ) -> SourceOutput:
        """Core Job lifecycle for source execution."""
        namespace = self._config.namespace
        job_name_to_watch = None

        try:
            # Check for existing Jobs
            existing = await self._check_existing_job(
                batch_api, namespace, convention_srn, source.digest
            )

            if existing == "succeeded":
                return self._parse_source_output(work_dir, files_dir)

            if existing and existing.startswith("active:"):
                job_name_to_watch = existing.split(":", 1)[1]
            else:
                spec = self._build_job_spec(
                    source,
                    work_dir=work_dir,
                    files_dir=files_dir,
                    inputs=inputs,
                    convention_srn=convention_srn,
                )
                job_name_to_watch = spec.metadata.name

                await batch_api.create_namespaced_job(namespace, spec)
                logger.info(
                    "Created K8s source Job",
                    extra={
                        "job_name": job_name_to_watch,
                        "namespace": namespace,
                        "image": f"{source.image}@{source.digest}",
                    },
                )

            # Phase 1: Scheduling
            await self._wait_for_scheduling(core_api, job_name_to_watch, namespace)

            # Phase 2: Completion
            result = await self._wait_for_completion(
                batch_api,
                core_api,
                job_name_to_watch,
                namespace,
                timeout_seconds=source.limits.timeout_seconds + 30,
            )

            if result == "succeeded":
                output = self._parse_source_output(work_dir, files_dir)
                logger.info(
                    "Source completed",
                    extra={
                        "job_name": job_name_to_watch,
                        "record_count": len(output.records),
                        "has_session": output.session is not None,
                    },
                )
                return output

            # Failed — diagnose and raise
            await self._diagnose_and_raise(core_api, job_name_to_watch, namespace, source, result)
            # unreachable but satisfies type checker
            raise ExternalServiceError("Source failed")

        finally:
            if job_name_to_watch:
                await self._cleanup_job(batch_api, job_name_to_watch, namespace)

    def _parse_source_output(self, work_dir: Path, files_dir: Path) -> SourceOutput:
        output_dir = work_dir / "output"
        records = parse_records_file(output_dir)
        session = parse_session_file(output_dir)
        return SourceOutput(records=records, session=session, files_dir=files_dir)

    async def _check_existing_job(
        self,
        batch_api: BatchV1Api,
        namespace: str,
        convention_srn: ConventionSRN | None,
        digest: str = "",
    ) -> str | None:
        label_parts = ["osa.io/role=source"]
        if convention_srn is not None:
            label_parts.append(f"osa.io/convention={label_value(convention_srn)}")
        if digest:
            label_parts.append(f"osa.io/digest={sanitize_label(digest)}")
        label_selector = ",".join(label_parts)

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
        source: SourceDefinition,
        *,
        work_dir: Path,
        files_dir: Path,
        inputs: SourceInputs | None = None,
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
            V1PodTemplateSpec,
            V1ResourceRequirements,
            V1SecurityContext,
            V1Volume,
            V1VolumeMount,
        )

        name = job_name("source", "src", str(convention_srn) if convention_srn else "unknown")
        relative_work = self._relative_path(work_dir)
        input_subpath = f"{relative_work}/input"
        output_subpath = f"{relative_work}/output"
        relative_files = self._relative_path(files_dir)

        labels: dict[str, str] = {
            "osa.io/role": "source",
            "osa.io/digest": sanitize_label(source.digest),
        }
        if convention_srn is not None:
            labels["osa.io/convention"] = label_value(convention_srn)

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
            name="source",
            image=f"{source.image}@{source.digest}",
            env=env,
            resources=V1ResourceRequirements(
                limits={"memory": source.limits.memory, "cpu": source.limits.cpu},
            ),
            security_context=V1SecurityContext(
                capabilities=V1Capabilities(drop=["ALL"]),
                allow_privilege_escalation=False,
                run_as_user=65534,
                run_as_group=65534,
            ),
            volume_mounts=mounts,
        )

        pod_spec = V1PodSpec(
            restart_policy="Never",
            automount_service_account_token=False,
            security_context=V1PodSecurityContext(run_as_non_root=True),
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
                active_deadline_seconds=SCHEDULING_TIMEOUT + source.limits.timeout_seconds,
                ttl_seconds_after_finished=self._config.job_ttl_seconds,
                template=V1PodTemplateSpec(
                    metadata=V1ObjectMeta(labels=labels),
                    spec=pod_spec,
                ),
            ),
        )

    def _relative_path(self, path: Path) -> str:
        mount = self._config.data_mount_path.rstrip("/")
        path_str = str(path)
        if not path_str.startswith(mount):
            raise ValueError(f"Path {path} is outside the data mount prefix {mount}")
        return path_str[len(mount) :].lstrip("/")

    async def _wait_for_scheduling(
        self,
        core_api: CoreV1Api,
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
                pod_list = await core_api.list_namespaced_pod(
                    namespace, label_selector=label_selector
                )
            except Exception as exc:
                raise classify_api_error(exc) from exc

            for pod in pod_list.items:
                phase = pod.status.phase
                if phase == "Failed":
                    reason = getattr(pod.status, "reason", None) or "Unknown"
                    raise InfrastructureError(f"Pod failed during scheduling: {reason}")

                if phase == "Pending" and pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        waiting = getattr(cs.state, "waiting", None)
                        if waiting and waiting.reason in ("ImagePullBackOff", "ErrImagePull"):
                            raise InfrastructureError(
                                f"Image pull failed: {waiting.reason}: {getattr(waiting, 'message', '')}"
                            )

                if phase in ("Running", "Succeeded", "Failed"):
                    return

            await asyncio.sleep(poll_interval)

        raise InfrastructureError(
            f"Pod scheduling timeout after {timeout_seconds}s for Job {job_name}"
        )

    async def _wait_for_completion(
        self,
        batch_api: BatchV1Api,
        core_api: CoreV1Api,
        job_name: str,
        namespace: str,
        *,
        timeout_seconds: float = 3630,
        poll_interval: float = 5.0,
    ) -> str:
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

        # Timed out — poll once more to catch last-millisecond completions
        try:
            job = await batch_api.read_namespaced_job(job_name, namespace)
            if job.status.succeeded:
                return "succeeded"
        except Exception:
            pass

        return "failed:WatchTimeout"

    async def _diagnose_and_raise(
        self,
        core_api: CoreV1Api,
        job_name: str,
        namespace: str,
        source: SourceDefinition,
        failure_info: str,
    ) -> None:
        """Determine failure reason and raise appropriate error."""
        if "DeadlineExceeded" in failure_info:
            raise ExternalServiceError(f"Source timed out after {source.limits.timeout_seconds}s")

        try:
            label_selector = f"job-name={job_name}"
            pod_list = await core_api.list_namespaced_pod(namespace, label_selector=label_selector)
            for pod in pod_list.items:
                if pod.status.container_statuses:
                    for cs in pod.status.container_statuses:
                        terminated = getattr(cs.state, "terminated", None)
                        if terminated:
                            if getattr(terminated, "reason", None) == "OOMKilled":
                                raise ExternalServiceError("Source killed by OOM")
                            exit_code = getattr(terminated, "exit_code", -1)
                            if exit_code != 0:
                                raise ExternalServiceError(f"Source exited with code {exit_code}")
        except ExternalServiceError:
            raise
        except Exception:
            pass

        raise ExternalServiceError(f"Source failed: {failure_info}")

    async def _cleanup_job(self, batch_api: BatchV1Api, job_name: str, namespace: str) -> None:
        try:
            await batch_api.delete_namespaced_job(
                job_name,
                namespace,
                propagation_policy="Background",
            )
        except Exception as exc:
            if getattr(exc, "status", None) == 404:
                return
            logger.warning("Failed to clean up K8s source Job", extra={"job_name": job_name})
