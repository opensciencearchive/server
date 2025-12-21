"""Validation service for running validation checks."""

import uuid
from datetime import datetime, timezone

from osa.domain.shared.model.srn import Domain, LocalId, ValidationRunSRN
from osa.domain.shared.service import Service
from osa.domain.validation.model import (
    CheckResult,
    CheckStatus,
    RunStatus,
    ValidationRun,
)
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.domain.validation.port.runner import (
    ResourceLimits,
    ValidationInputs,
    ValidatorRunner,
)


class ValidationService(Service):
    """Orchestrates validation runs."""

    run_repo: ValidationRunRepository
    runner: ValidatorRunner
    node_domain: Domain

    async def create_run(
        self,
        inputs: ValidationInputs,
        expires_at: datetime | None = None,
    ) -> ValidationRun:
        """Create a new validation run."""
        run_srn = ValidationRunSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid.uuid4())),
            version=None,
        )

        run = ValidationRun(
            srn=run_srn,
            status=RunStatus.PENDING,
            results=[],
            started_at=None,
            completed_at=None,
            expires_at=expires_at,
        )
        await self.run_repo.save(run)
        return run

    async def run_validation(
        self,
        run: ValidationRun,
        inputs: ValidationInputs,
        validators: list[tuple[str, str]],  # List of (image, digest) pairs
    ) -> ValidationRun:
        """
        Execute validation checks for a run.

        Args:
            run: The validation run to execute
            inputs: The data to validate
            validators: List of (image, digest) pairs for validators to run

        Returns:
            Updated ValidationRun with results
        """
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        results: list[CheckResult] = []
        overall_failed = False

        for image, digest in validators:
            result = await self._run_validator(image, digest, inputs)
            results.append(result)

            if result.status in (CheckStatus.FAILED, CheckStatus.ERROR):
                overall_failed = True

        run.results = results
        run.status = RunStatus.FAILED if overall_failed else RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        return run

    async def _run_validator(
        self,
        image: str,
        digest: str,
        inputs: ValidationInputs,
    ) -> CheckResult:
        """Run a single validator."""
        try:
            output = await self.runner.run(
                image=image,
                digest=digest,
                inputs=inputs,
                timeout=60,
                resources=ResourceLimits(memory="256Mi", cpu="0.5"),
            )

            return CheckResult(
                check_id=f"{image}@{digest[:12]}",
                validator_digest=digest,
                status=output.status,
                message=output.error,
                details={"checks": output.checks} if output.checks else None,
            )

        except Exception as e:
            return CheckResult(
                check_id=f"{image}@{digest[:12] if digest else 'unknown'}",
                validator_digest=digest or "",
                status=CheckStatus.ERROR,
                message=str(e),
                details=None,
            )

    async def get_run(self, run_id: str) -> ValidationRun | None:
        """Get a validation run by its ID (local part of SRN)."""
        run_srn = ValidationRunSRN(
            domain=self.node_domain,
            id=LocalId(run_id),
            version=None,
        )
        return await self.run_repo.get(run_srn)
