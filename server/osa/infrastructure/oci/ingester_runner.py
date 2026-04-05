"""OCI ingester runner using aiodocker."""

import asyncio
import json
import os
import stat
import time
from pathlib import Path

import aiodocker
from osa.domain.shared.error import OOMError, TransientError
from osa.infrastructure.logging import get_logger
from osa.domain.shared.model.source import IngesterDefinition
from osa.domain.shared.port.ingester_runner import IngesterInputs, IngesterOutput, IngesterRunner
from osa.infrastructure.runner_utils import (
    parse_memory,
    parse_records_file,
    parse_session_file,
)


log = get_logger(__name__)


class OciIngesterRunner(IngesterRunner):
    """Executes ingesters in OCI containers via aiodocker.

    Key differences from OciHookRunner:
    - Network access enabled (ingesters call upstream APIs)
    - Three bind mounts: $OSA_IN (ro), $OSA_OUT (rw), $OSA_FILES (rw)
    - No ReadonlyRootfs (ingesters may need writable FS for pip cache, etc.)
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

    async def capture_logs(self, run_id: str) -> str:
        """OCI containers are deleted after run — logs captured inline during execution."""
        return ""

    async def run(
        self,
        ingester: IngesterDefinition,
        inputs: IngesterInputs,
        files_dir: Path,
        work_dir: Path,
    ) -> IngesterOutput:
        timeout = ingester.limits.timeout_seconds

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
            if inputs.config or ingester.config:
                config = {**(ingester.config or {}), **(inputs.config or {})}
                (staging_dir / "config.json").write_text(json.dumps(config))

            if inputs.session:
                (staging_dir / "session.json").write_text(json.dumps(inputs.session))

            start_time = time.monotonic()

            try:

                async def _resolve_and_run():
                    image_ref = await self._resolve_image(ingester.image, ingester.digest)
                    return await self._run_container(
                        image_ref, staging_dir, files_dir, container_output, ingester, inputs
                    )

                result = await asyncio.wait_for(
                    _resolve_and_run(),
                    timeout=timeout,
                )
                return result
            except asyncio.TimeoutError:
                duration = time.monotonic() - start_time
                log.error(
                    "Ingester timed out after {timeout}s",
                    image=ingester.image,
                    timeout=timeout,
                    duration=duration,
                )
                raise TransientError(f"Ingester timed out after {timeout}s")
        finally:
            rmtree(staging_dir, onexc=_force_remove)

    async def _run_container(
        self,
        image_ref: str,
        staging_dir: Path,
        files_dir: Path,
        output_dir: Path,
        ingester: IngesterDefinition,
        inputs: IngesterInputs,
    ) -> IngesterOutput:
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
                    "Memory": parse_memory(ingester.limits.memory),
                    "MemorySwap": parse_memory(ingester.limits.memory),
                    "NanoCpus": int(float(ingester.limits.cpu) * 1e9),
                    # No NetworkMode: "none" — sources need network access
                    # No ReadonlyRootfs — ingesters may need writable FS
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
                raise OOMError("Ingester killed by OOM")

            if exit_code != 0:
                logs = await container.log(stdout=True, stderr=True)
                logs_str = "".join(logs) if logs else ""
                log.error(
                    "Ingester exited with code {exit_code}",
                    exit_code=exit_code,
                    image=ingester.image,
                    container_logs=logs_str[:2000],
                )
                raise TransientError(f"Ingester exited with code {exit_code}")

            records = parse_records_file(output_dir)
            session = parse_session_file(output_dir)
            return IngesterOutput(records=records, session=session, files_dir=files_dir)

        except aiodocker.DockerError as e:
            log.error("Docker error running ingester: {error}", error=str(e))
            raise TransientError(f"Docker error: {e}") from e
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
        log.info("Pulling ingester image: {image}", image=image)
        await self._docker.images.pull(image)
        return image
