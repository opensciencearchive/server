"""OCI hook runner using aiodocker."""

import asyncio
import json
import os
import stat
import sys
import time
from pathlib import Path
from shutil import rmtree

import aiodocker
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.infrastructure.logging import get_logger
from osa.infrastructure.runner_utils import (
    detect_rejection,
    parse_memory,
    parse_progress_file,
)


def _force_remove(func, path, exc):
    """rmtree onexc handler: fix permissions left by Docker containers, then retry."""
    os.chmod(path, stat.S_IRWXU)
    func(path)


log = get_logger(__name__)


class OciHookRunner(HookRunner):
    """Executes hooks in OCI containers via aiodocker."""

    def __init__(
        self,
        docker: aiodocker.Docker,
        host_data_dir: str | None = None,
        container_data_dir: str = "/data",
    ):
        self._docker = docker
        self._host_data_dir = host_data_dir
        self._container_data_dir = container_data_dir

    async def run(
        self,
        hook: HookDefinition,
        inputs: HookInputs,
        work_dir: Path,
    ) -> HookResult:
        timeout = hook.runtime.limits.timeout_seconds

        # Create sibling input/ and output/ dirs under work_dir
        staging_dir = work_dir / "input"
        staging_dir.mkdir(parents=True, exist_ok=True)
        container_output = work_dir / "output"
        container_output.mkdir(parents=True, exist_ok=True)
        try:
            # Write records.jsonl (unified batch contract)
            with (staging_dir / "records.jsonl").open("w") as f:
                for record in inputs.records:
                    f.write(json.dumps(record.model_dump()) + "\n")

            if inputs.config or hook.runtime.config:
                config = {**hook.runtime.config, **(inputs.config or {})}
                (staging_dir / "config.json").write_text(json.dumps(config))

            # Create files directory structure: $OSA_FILES/{id}/ per record
            files_base = staging_dir / "files"
            files_base.mkdir(exist_ok=True)

            start_time = time.monotonic()

            try:

                async def _resolve_and_run():
                    image_ref = await self._resolve_image(hook.runtime.image, hook.runtime.digest)
                    return await self._run_container(
                        image_ref,
                        staging_dir,
                        inputs.files_dirs,
                        container_output,
                        hook,
                        files_base,
                    )

                result = await asyncio.wait_for(
                    _resolve_and_run(),
                    timeout=timeout,
                )
                result_duration = time.monotonic() - start_time
                return HookResult(
                    hook_name=hook.name,
                    status=result["status"],
                    rejection_reason=result.get("rejection_reason"),
                    error_message=result.get("error_message"),
                    progress=result.get("progress", []),
                    duration_seconds=result_duration,
                )
            except asyncio.TimeoutError:
                duration = time.monotonic() - start_time
                log.error(
                    "Hook timed out",
                    hook=hook.name,
                    run_id=inputs.run_id,
                    timeout=timeout,
                )
                return HookResult(
                    hook_name=hook.name,
                    status=HookStatus.FAILED,
                    error_message=f"Hook timed out after {timeout}s",
                    duration_seconds=duration,
                )
        finally:
            rmtree(staging_dir, onexc=_force_remove)

    async def _run_container(
        self,
        image_ref: str,
        staging_dir: Path,
        files_dirs: dict[str, Path],
        output_dir: Path,
        hook: HookDefinition,
        files_base: Path,
    ) -> dict:
        container = None
        try:
            # Bind mounts: staging at /osa/in:ro, files at /osa/files:ro, output at /osa/out:rw
            binds = [
                f"{self._host_path(staging_dir)}:/osa/in:ro",
                f"{self._host_path(output_dir)}:/osa/out:rw",
            ]

            # Mount per-record file directories under /osa/files/{id}/
            # Sanitize IDs to avoid colons breaking Docker's bind mount syntax
            if files_dirs:
                for record_id, fdir in files_dirs.items():
                    if fdir and fdir.exists():
                        safe_id = record_id.replace(":", "_").replace("@", "_")
                        binds.append(f"{self._host_path(fdir)}:/osa/files/{safe_id}:ro")
            elif files_base.exists():
                binds.append(f"{self._host_path(files_base)}:/osa/files:ro")

            # todo: use pydantic
            config = {
                "Image": image_ref,
                "Env": [
                    "OSA_IN=/osa/in",
                    "OSA_OUT=/osa/out",
                    "OSA_FILES=/osa/files",
                    f"OSA_HOOK_NAME={hook.name}",
                ],
                "User": "65534:65534",
                "HostConfig": {
                    "Binds": binds,
                    "Memory": parse_memory(hook.runtime.limits.memory),
                    "MemorySwap": parse_memory(hook.runtime.limits.memory),
                    "NanoCpus": int(float(hook.runtime.limits.cpu) * 1e9),
                    "NetworkMode": "none",
                    "ReadonlyRootfs": True,
                    "CapDrop": ["ALL"],
                    "SecurityOpt": ["no-new-privileges"],
                    "PidsLimit": 256,
                    "Tmpfs": {"/tmp": "rw,noexec,nosuid,size=100m"},
                },
            }

            container = await self._docker.containers.create(config)
            await container.start()
            wait_result = await container.wait()

            exit_code = wait_result.get("StatusCode", -1)

            # Check OOM
            inspect_data = await container.show()
            oom_killed = inspect_data.get("State", {}).get("OOMKilled", False)

            if oom_killed:
                # Grab tail of container logs before deletion
                try:
                    tail_logs = await container.log(stdout=True, stderr=True, tail=3)
                    tail_text = "".join(tail_logs).strip() if tail_logs else ""
                except Exception:
                    tail_text = ""
                log.error(
                    "OOM: hook={hook_name} limit={memory}",
                    hook_name=hook.name,
                    memory=hook.runtime.limits.memory,
                )
                if tail_text:
                    for line in tail_text.splitlines():
                        print(f"    OOM [{hook.name}] {line}", file=sys.stderr, flush=True)
                return {
                    "status": HookStatus.FAILED,
                    "error_message": f"Hook killed by OOM (limit: {hook.runtime.limits.memory})",
                }

            # Parse progress file
            progress = parse_progress_file(output_dir)

            # Check for rejection in progress
            rejected, rejection = detect_rejection(progress)
            if rejected:
                return {
                    "status": HookStatus.REJECTED,
                    "rejection_reason": rejection,
                    "progress": progress,
                }

            if exit_code != 0:
                logs = await container.log(stdout=True, stderr=True)
                logs_str = "".join(logs) if logs else ""
                return {
                    "status": HookStatus.FAILED,
                    "error_message": f"Hook exited with code {exit_code}: {logs_str[:2000]}",
                    "progress": progress,
                }

            return {
                "status": HookStatus.PASSED,
                "progress": progress,
            }

        except aiodocker.DockerError as e:
            log.error("Docker error running hook", error=str(e))
            return {
                "status": HookStatus.FAILED,
                "error_message": f"Docker error: {e}",
            }
        except Exception as e:
            log.error("Unexpected error running hook", error=str(e))
            return {
                "status": HookStatus.FAILED,
                "error_message": f"Unexpected error: {e}",
            }
        finally:
            if container is not None:
                try:
                    await container.delete(force=True)
                except Exception:
                    pass

    def _host_path(self, container_path: Path) -> str:
        """Translate a container-internal path to a host path for bind mounts."""
        path_str = str(container_path)
        if self._host_data_dir:
            path_str = path_str.replace(self._container_data_dir, self._host_data_dir, 1)
        return path_str

    async def _resolve_image(self, image: str, digest: str) -> str:
        """Resolve an image reference, preferring local tag over registry pull."""
        # Try the tag first (works for locally-built images)
        try:
            await self._docker.images.inspect(image)
            return image
        except aiodocker.DockerError:
            pass

        # Try digest reference
        digest_ref = f"{image}@{digest}"
        try:
            await self._docker.images.inspect(digest_ref)
            return digest_ref
        except aiodocker.DockerError:
            pass

        # Pull from registry as last resort
        log.info("Pulling hook image", image=image)
        await self._docker.images.pull(image)
        return image
