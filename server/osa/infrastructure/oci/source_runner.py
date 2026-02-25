"""OCI source runner using aiodocker."""

import asyncio
import json
import os
import re
import stat
import time
from pathlib import Path

import aiodocker
import logfire

from osa.domain.shared.error import ExternalServiceError
from osa.domain.shared.model.source import SourceDefinition
from osa.domain.source.port.source_runner import SourceInputs, SourceOutput, SourceRunner


class OciSourceRunner(SourceRunner):
    """Executes sources in OCI containers via aiodocker.

    Key differences from OciHookRunner:
    - Network access enabled (sources call upstream APIs)
    - Three bind mounts: $OSA_IN (ro), $OSA_OUT (rw), $OSA_FILES (rw)
    - No ReadonlyRootfs (sources may need writable FS for pip cache, etc.)
    - Higher default limits (3600s timeout, 4g memory)
    - Output is records.jsonl (line-delimited JSON), not features.json

    When running inside a container with the Docker socket mounted (sibling containers),
    set ``host_data_dir`` and ``container_data_dir`` so bind mount paths are translated
    from container-internal paths to host paths that the Docker daemon can resolve.
    """

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
        source: SourceDefinition,
        inputs: SourceInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> SourceOutput:
        timeout = source.limits.timeout_seconds

        from shutil import rmtree

        def _force_remove(func, path, exc):
            os.chmod(path, stat.S_IRWXU)
            func(path)

        files_dir.mkdir(parents=True, exist_ok=True)

        # Create sibling input/ and output/ dirs under work_dir
        staging_dir = work_dir / "input"
        staging_dir.mkdir(parents=True, exist_ok=True)
        container_output = work_dir / "output"
        container_output.mkdir(parents=True, exist_ok=True)
        try:
            if inputs.config or source.config:
                config = {**(source.config or {}), **(inputs.config or {})}
                (staging_dir / "config.json").write_text(json.dumps(config))

            if inputs.session:
                (staging_dir / "session.json").write_text(json.dumps(inputs.session))

            start_time = time.monotonic()

            try:

                async def _resolve_and_run():
                    image_ref = await self._resolve_image(source.image, source.digest)
                    return await self._run_container(
                        image_ref, staging_dir, files_dir, container_output, source, inputs
                    )

                result = await asyncio.wait_for(
                    _resolve_and_run(),
                    timeout=timeout,
                )
                return result
            except asyncio.TimeoutError:
                duration = time.monotonic() - start_time
                logfire.error(
                    "Source timed out",
                    image=source.image,
                    timeout=timeout,
                    duration=duration,
                )
                raise ExternalServiceError(f"Source timed out after {timeout}s")
        finally:
            rmtree(staging_dir, onexc=_force_remove)

    async def _run_container(
        self,
        image_ref: str,
        staging_dir: Path,
        files_dir: Path,
        output_dir: Path,
        source: SourceDefinition,
        inputs: SourceInputs,
    ) -> SourceOutput:
        container = None
        try:
            # Build env vars
            env = [
                "OSA_IN=/osa/in",
                "OSA_OUT=/osa/out",
                "OSA_FILES=/osa/files",
            ]
            if inputs.since is not None:
                env.append(f"OSA_SINCE={inputs.since.isoformat()}")
            if inputs.limit is not None:
                env.append(f"OSA_LIMIT={inputs.limit}")
            if inputs.offset:
                env.append(f"OSA_OFFSET={inputs.offset}")

            binds = [
                f"{self._host_path(staging_dir)}:/osa/in:ro",
                f"{self._host_path(output_dir)}:/osa/out:rw",
                f"{self._host_path(files_dir)}:/osa/files:rw",
            ]

            config = {
                "Image": image_ref,
                "Env": env,
                "HostConfig": {
                    "Binds": binds,
                    "Memory": self._parse_memory(source.limits.memory),
                    "MemorySwap": self._parse_memory(source.limits.memory),
                    "NanoCpus": int(float(source.limits.cpu) * 1e9),
                    # No NetworkMode: "none" — sources need network access
                    # No ReadonlyRootfs — sources may need writable FS
                    "CapDrop": ["ALL"],
                    "SecurityOpt": ["no-new-privileges"],
                    "PidsLimit": 256,
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
                raise ExternalServiceError("Source killed by OOM")

            if exit_code != 0:
                logs = await container.log(stdout=True, stderr=True)
                logs_str = "".join(logs) if logs else ""
                raise ExternalServiceError(f"Source exited with code {exit_code}: {logs_str[:500]}")

            return self._parse_output(output_dir, files_dir)

        except aiodocker.DockerError as e:
            logfire.error("Docker error running source", error=str(e))
            raise ExternalServiceError(f"Docker error: {e}") from e
        finally:
            if container is not None:
                try:
                    await container.delete(force=True)
                except Exception:
                    pass

    def _host_path(self, container_path: Path) -> str:
        """Translate a container-internal path to a host path for bind mounts.

        When running as a sibling container (Docker socket mounted), paths inside
        this container are meaningless to the host Docker daemon. This method
        replaces the container data dir prefix with the host data dir.
        """
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
        logfire.info("Pulling source image", image=image)
        await self._docker.images.pull(image)
        return image

    def _parse_output(self, output_dir: Path, files_dir: Path) -> SourceOutput:
        """Parse records.jsonl and session.json from the output directory."""
        records: list[dict] = []
        records_file = output_dir / "records.jsonl"
        if records_file.exists():
            for line in records_file.read_text().strip().split("\n"):
                if not line.strip():
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    logfire.warn("Skipping invalid JSON line in records.jsonl")
                    continue

        session = None
        session_file = output_dir / "session.json"
        if session_file.exists():
            try:
                session = json.loads(session_file.read_text())
            except json.JSONDecodeError:
                logfire.warn("Invalid session.json")

        return SourceOutput(records=records, session=session, files_dir=files_dir)

    def _parse_memory(self, memory: str) -> int:
        """Parse memory string like '4g' or '512m' to bytes."""
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
