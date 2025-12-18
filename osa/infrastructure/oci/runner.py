import asyncio
import json
import re
import tempfile
from pathlib import Path
from shutil import copytree
from typing import Literal

import aiodocker
import logfire
from pydantic import BaseModel

from osa.domain.validation.model import CheckStatus
from osa.domain.validation.port.runner import (
    ResourceLimits,
    ValidationInputs,
    ValidatorOutput,
    ValidatorRunner,
)


class ValidatorError(Exception):
    """Raised when a validator container fails to run."""

    pass


class HostConfig(BaseModel):
    """Docker host configuration for container resource limits and mounts."""

    Binds: list[str]
    Memory: int
    NanoCpus: int
    NetworkMode: Literal["none", "bridge", "host"] = "none"


class ContainerConfig(BaseModel):
    """Docker container configuration for running validators."""

    Image: str
    Env: list[str]
    HostConfig: HostConfig
    User: str = "nobody"


class DockerValidatorRunner(ValidatorRunner):
    """Executes OCI validators using Docker via aiodocker."""

    def __init__(self, docker: aiodocker.Docker):
        self._docker = docker

    async def run(
        self,
        image: str,
        digest: str,
        inputs: ValidationInputs,
        timeout: int,
        resources: ResourceLimits,
    ) -> ValidatorOutput:
        """
        Run a validator container.

        Sets up the $OSAP_IN and $OSAP_OUT filesystem contract:
        - $OSAP_IN/record.json - the data payload
        - $OSAP_IN/files/ - associated files (if any)
        - $OSAP_IN/config.json - optional per-run configuration
        - $OSAP_OUT/result.json - validator output

        Args:
            image: OCI image reference
            digest: Image digest for reproducibility
            inputs: Data to validate
            timeout: Timeout in seconds
            resources: CPU/memory limits

        Returns:
            ValidatorOutput with status and check results
        """
        image_ref = f"{image}@{digest}"

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            osap_in = tmpdir_path / "in"
            osap_out = tmpdir_path / "out"
            osap_in.mkdir()
            osap_out.mkdir()

            # Write inputs
            (osap_in / "record.json").write_text(json.dumps(inputs.record_json))

            if inputs.files_dir and inputs.files_dir.exists():
                copytree(inputs.files_dir, osap_in / "files")

            if inputs.config:
                (osap_in / "config.json").write_text(json.dumps(inputs.config))

            try:
                return await asyncio.wait_for(
                    self._run_container(
                        self._docker, image_ref, osap_in, osap_out, resources
                    ),
                    timeout=timeout,
                )
            except asyncio.TimeoutError:
                logfire.error("Validator timed out", image=image, digest=digest)
                return ValidatorOutput(
                    status=CheckStatus.ERROR,
                    checks=[],
                    error=f"Validator timed out after {timeout}s",
                )

    async def _run_container(
        self,
        docker: aiodocker.Docker,
        image_ref: str,
        osap_in: Path,
        osap_out: Path,
        resources: ResourceLimits,
    ) -> ValidatorOutput:
        """Run the container asynchronously."""
        container = None
        try:
            # Pull image if needed
            try:
                await docker.images.inspect(image_ref)
            except aiodocker.DockerError as e:
                if e.status == 404:
                    logfire.info("Pulling validator image", image=image_ref)
                    await docker.images.pull(image_ref)
                else:
                    raise

            # Create container config
            config = ContainerConfig(
                Image=image_ref,
                Env=[
                    "OSAP_IN=/osap/in",
                    "OSAP_OUT=/osap/out",
                ],
                HostConfig=HostConfig(
                    Binds=[
                        f"{osap_in}:/osap/in:ro",
                        f"{osap_out}:/osap/out:rw",
                    ],
                    Memory=self._parse_memory(resources.memory),
                    NanoCpus=int(float(resources.cpu) * 1e9),
                    NetworkMode="none",
                ),
                User="nobody",
            )

            # Create and start container
            container = await docker.containers.create(config.model_dump())
            await container.start()

            # Wait for completion
            result = await container.wait()
            exit_code = result.get("StatusCode", -1)

            # Get logs for debugging
            logs = await container.log(stdout=True, stderr=True)
            logs_str = "".join(logs) if logs else ""

            if exit_code != 0:
                logfire.warning(
                    "Validator exited with non-zero code",
                    exit_code=exit_code,
                    logs=logs_str[:1000],
                )
                return ValidatorOutput(
                    status=CheckStatus.ERROR,
                    checks=[],
                    error=f"Validator exited with code {exit_code}: {logs_str[:500]}",
                )

            # Read result
            result_path = osap_out / "result.json"
            if not result_path.exists():
                return ValidatorOutput(
                    status=CheckStatus.ERROR,
                    checks=[],
                    error="Validator did not produce result.json",
                )

            result_data = json.loads(result_path.read_text())
            status_str = result_data.get("status", "error")
            checks = result_data.get("checks", [])

            # Map status string to CheckStatus
            status_map = {
                "passed": CheckStatus.PASSED,
                "warnings": CheckStatus.WARNINGS,
                "failed": CheckStatus.FAILED,
            }
            status = status_map.get(status_str, CheckStatus.ERROR)

            return ValidatorOutput(status=status, checks=checks, error=None)

        except aiodocker.DockerError as e:
            logfire.error("Docker error", error=str(e))
            return ValidatorOutput(
                status=CheckStatus.ERROR,
                checks=[],
                error=f"Docker error: {e}",
            )
        except Exception as e:
            logfire.error("Unexpected error running validator", error=str(e))
            return ValidatorOutput(
                status=CheckStatus.ERROR,
                checks=[],
                error=f"Unexpected error: {e}",
            )
        finally:
            # Clean up container
            if container is not None:
                try:
                    await container.delete(force=True)
                except Exception:
                    logfire.warning("Failed to delete container", container_id=container.id)
                    pass

    def _parse_memory(self, memory: str) -> int:
        """Parse memory string like '256Mi' to bytes."""
        memory = memory.strip()
        Gi = 1024 * 1024 * 1024
        Mi = 1024 * 1024
        Ki = 1024
        regex = r"^\d+(Gi|Mi|Ki)?$"
        if not re.match(regex, memory):
            raise ValueError(f"Invalid memory format: {memory}")

        match = re.match(r"^(\d+)(Gi|Mi|Ki)?$", memory)
        if not match:
            raise ValueError(f"Invalid memory format: {memory}")
        amount, unit = match.groups()

        match unit:
            case "Gi":
                return int(amount) * Gi
            case "Mi":
                return int(amount) * Mi
            case "Ki":
                return int(amount) * Ki
            case None:
                logfire.warn("Memory unit not specified, assuming bytes", memory=memory)
                return int(amount)
            case _:
                raise ValueError(f"Unknown memory unit: {unit}")
