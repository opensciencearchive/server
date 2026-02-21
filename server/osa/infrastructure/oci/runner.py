"""OCI hook runner using aiodocker."""

import asyncio
import json
import re
import time
from pathlib import Path
from shutil import copytree

import aiodocker
import logfire

from osa.domain.shared.model.hook import HookDefinition
from osa.domain.validation.model.hook_result import HookResult, HookStatus, ProgressEntry
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner


class OciHookRunner(HookRunner):
    """Executes hooks in OCI containers via aiodocker."""

    def __init__(self, docker: aiodocker.Docker):
        self._docker = docker

    async def run(
        self,
        hook: HookDefinition,
        inputs: HookInputs,
        workspace_dir: Path,
    ) -> HookResult:
        image_ref = f"{hook.image}@{hook.digest}"
        timeout = hook.limits.timeout_seconds

        osa_in = workspace_dir / "in"
        osa_out = workspace_dir / "out"
        osa_in.mkdir(parents=True, exist_ok=True)
        osa_out.mkdir(parents=True, exist_ok=True)

        # Stage inputs
        (osa_in / "record.json").write_text(json.dumps(inputs.record_json))

        if inputs.files_dir and inputs.files_dir.exists():
            copytree(inputs.files_dir, osa_in / "files")

        if inputs.config or hook.config:
            config = {**(hook.config or {}), **(inputs.config or {})}
            (osa_in / "config.json").write_text(json.dumps(config))

        start_time = time.monotonic()

        try:
            result = await asyncio.wait_for(
                self._run_container(image_ref, osa_in, osa_out, hook),
                timeout=timeout,
            )
            result_duration = time.monotonic() - start_time
            return HookResult(
                hook_name=hook.manifest.name,
                status=result["status"],
                features=result["features"],
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
                features=[],
                error_message=f"Hook timed out after {timeout}s",
                duration_seconds=duration,
            )

    async def _run_container(
        self,
        image_ref: str,
        osa_in: Path,
        osa_out: Path,
        hook: HookDefinition,
    ) -> dict:
        container = None
        try:
            # Pull image if needed
            try:
                await self._docker.images.inspect(image_ref)
            except aiodocker.DockerError as e:
                if e.status == 404:
                    logfire.info("Pulling hook image", image=image_ref)
                    await self._docker.images.pull(image_ref)
                else:
                    raise

            config = {
                "Image": image_ref,
                "Env": ["OSA_IN=/osa/in", "OSA_OUT=/osa/out"],
                "User": "65534:65534",
                "HostConfig": {
                    "Binds": [
                        f"{osa_in}:/osa/in:ro",
                        f"{osa_out}:/osa/out:rw",
                    ],
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
                    "features": [],
                    "error_message": "Hook killed by OOM",
                }

            # Parse progress file
            progress = self._parse_progress(osa_out)

            # Check for rejection in progress
            rejection = self._check_rejection(progress)
            if rejection:
                return {
                    "status": HookStatus.REJECTED,
                    "features": [],
                    "rejection_reason": rejection,
                    "progress": progress,
                }

            if exit_code != 0:
                logs = await container.log(stdout=True, stderr=True)
                logs_str = "".join(logs) if logs else ""
                return {
                    "status": HookStatus.FAILED,
                    "features": [],
                    "error_message": f"Hook exited with code {exit_code}: {logs_str[:500]}",
                    "progress": progress,
                }

            # Collect features
            features = self._collect_features(osa_out, hook)

            return {
                "status": HookStatus.PASSED,
                "features": features,
                "progress": progress,
            }

        except aiodocker.DockerError as e:
            logfire.error("Docker error running hook", error=str(e))
            return {
                "status": HookStatus.FAILED,
                "features": [],
                "error_message": f"Docker error: {e}",
            }
        except Exception as e:
            logfire.error("Unexpected error running hook", error=str(e))
            return {
                "status": HookStatus.FAILED,
                "features": [],
                "error_message": f"Unexpected error: {e}",
            }
        finally:
            if container is not None:
                try:
                    await container.delete(force=True)
                except Exception:
                    pass

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

    def _collect_features(self, osa_out: Path, hook: HookDefinition) -> list[dict]:
        """Collect features from features.json."""
        features_file = osa_out / "features.json"
        if not features_file.exists():
            return []

        data = json.loads(features_file.read_text())
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        return []

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
