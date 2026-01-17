from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from osa.domain.shared.port import Port
from osa.domain.validation.model.value import CheckStatus


@dataclass(frozen=True)
class ValidationInputs:
    """Inputs to pass to a validator container."""

    record_json: dict
    files_dir: Path | None = None
    config: dict | None = None


@dataclass(frozen=True)
class ValidatorOutput:
    """Output from a validator container."""

    status: CheckStatus
    checks: list[dict]
    error: str | None = None


@dataclass(frozen=True)
class ResourceLimits:
    """Resource limits for validator container."""

    memory: str = "256Mi"
    cpu: str = "0.5"


@runtime_checkable
class ValidatorRunner(Port, Protocol):
    """Execute OCI validator containers."""

    @abstractmethod
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

        Args:
            image: OCI image reference (e.g., ghcr.io/osap/validators/si-units)
            digest: Image digest for reproducibility (e.g., sha256:abc123...)
            inputs: Data to validate
            timeout: Timeout in seconds
            resources: CPU/memory limits

        Returns:
            ValidatorOutput with status and check results

        Raises:
            ValidatorError: If container fails to run (not if validation fails)
        """
        ...
