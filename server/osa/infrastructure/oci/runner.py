"""OCI hook runner using aiodocker."""

import asyncio
import json
import os
import re
import stat
import time
from pathlib import Path
from shutil import rmtree

import aiodocker
import logfire

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.validation.model.hook_result import HookResult, HookStatus, ProgressEntry
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner


def _force_remove(func, path, exc):
    """rmtree onexc handler: fix permissions left by Docker containers, then retry."""
    os.chmod(path, stat.S_IRWXU)
    func(path)


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
        timeout = hook.limits.timeout_seconds

        # Create sibling input/ and output/ dirs under work_dir
        staging_dir = work_dir / "input"
        staging_dir.mkdir(parents=True, exist_ok=True)
        container_output = work_dir / "output"
        container_output.mkdir(parents=True, exist_ok=True)
        try:
            (staging_dir / "record.json").write_text(json.dumps(inputs.record_json))
            # Pre-create files mountpoint so nested bind works with ReadonlyRootfs
            (staging_dir / "files").mkdir(exist_ok=True)

            if inputs.config or hook.config:
                config = {**(hook.config or {}), **(inputs.config or {})}
                (staging_dir / "config.json").write_text(json.dumps(config))

            start_time = time.monotonic()

            try:

                async def _resolve_and_run():
                    image_ref = await self._resolve_image(hook.image, hook.digest)
                    return await self._run_container(
                        image_ref, staging_dir, inputs.files_dir, container_output, hook
                    )

                result = await asyncio.wait_for(
                    _resolve_and_run(),
                    timeout=timeout,
                )
                result_duration = time.monotonic() - start_time
                return HookResult(
                    hook_name=hook.manifest.name,
                    status=result["status"],
                    rejection_reason=result.get("rejection_reason"),
                    error_message=result.get("error_message"),
                    progress=result.get("progress", []),
                    duration_seconds=result_duration,
                )
            except asyncio.TimeoutError:
                duration = time.monotonic() - start_time
                logfire.error("Hook timed out", hook=hook.manifest.name, timeout=timeout)
                return HookResult(
                    hook_name=hook.manifest.name,
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
        files_dir: Path | None,
        output_dir: Path,
        hook: HookDefinition,
    ) -> dict:
        container = None
        try:
            # Nested bind-mounts: staging at /osa/in:ro, files at /osa/in/files:ro
            binds = [
                f"{self._host_path(staging_dir)}:/osa/in:ro",
                f"{self._host_path(output_dir)}:/osa/out:rw",
            ]
            if files_dir and files_dir.exists():
                binds.append(f"{self._host_path(files_dir)}:/osa/in/files:ro")

            # todo: use pydantic
            config = {
                "Image": image_ref,
                "Env": [
                    "OSA_IN=/osa/in",
                    "OSA_OUT=/osa/out",
                    f"OSA_HOOK_NAME={hook.manifest.name}",
                ],
                "User": "65534:65534",
                "HostConfig": {
                    "Binds": binds,
                    "Memory": self._parse_memory(hook.limits.memory),
                    "MemorySwap": self._parse_memory(hook.limits.memory),
                    "NanoCpus": int(float(hook.limits.cpu) * 1e9),
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
                return {
                    "status": HookStatus.FAILED,
                    "error_message": "Hook killed by OOM",
                }

            # Parse progress file
            progress = self._parse_progress(output_dir)

            # Check for rejection in progress
            rejection = self._check_rejection(progress)
            if rejection:
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
            logfire.error("Docker error running hook", error=str(e))
            return {
                "status": HookStatus.FAILED,
                "error_message": f"Docker error: {e}",
            }
        except Exception as e:
            logfire.error("Unexpected error running hook", error=str(e))
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
        logfire.info("Pulling hook image", image=image)
        await self._docker.images.pull(image)
        return image

    def _parse_progress(self, osa_out: Path) -> list[ProgressEntry]:
        """Parse progress.jsonl from hook output."""
        progress_file = osa_out / "progress.jsonl"
        if not progress_file.exists():
            return []

        entries = []
        for line in progress_file.read_text().strip().split("\n"):
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                entries.append(
                    ProgressEntry(
                        step=data.get("step"),
                        status=data.get("status", "unknown"),
                        message=data.get("message"),
                    )
                )
            except json.JSONDecodeError:
                continue
        return entries

    def _check_rejection(self, progress: list[ProgressEntry]) -> str | None:
        """Check if any progress entry indicates rejection."""
        for entry in reversed(progress):
            if entry.status == "rejected":
                return entry.message
        return None

    def _parse_memory(self, memory: str) -> int:
        """Parse memory string like '2g' or '512m' to bytes."""
        memory = memory.strip().lower()
        match = re.match(r"^(\d+(?:\.\d+)?)(g|m|k)?i?$", memory)
        if not match:
            raise ValueError(f"Invalid memory format: {memory}")

        amount = float(match.group(1))
        unit = match.group(2)

        match unit:
            case "g":
                return int(amount * 1024 * 1024 * 1024)
            case "m":
                return int(amount * 1024 * 1024)
            case "k":
                return int(amount * 1024)
            case None:
                return int(amount)
            case _:
                raise ValueError(f"Unknown memory unit: {unit}")
