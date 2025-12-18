"""Validation service for running data against traits."""

import uuid
from datetime import datetime, timezone

from osa.domain.shared.model.srn import Domain, LocalId, TraitSRN, ValidationRunSRN
from osa.domain.shared.service import Service
from osa.domain.validation.model import (
    CheckResult,
    CheckStatus,
    RunStatus,
    ValidationRun,
)
from osa.domain.validation.port.repository import TraitRepository, ValidationRunRepository
from osa.domain.validation.port.runner import (
    ResourceLimits,
    ValidationInputs,
    ValidatorRunner,
)


class ValidationService(Service):
    """Orchestrates validation of data against traits."""

    trait_repo: TraitRepository
    run_repo: ValidationRunRepository
    runner: ValidatorRunner
    node_domain: Domain

    async def validate(
        self,
        trait_srns: list[TraitSRN],
        inputs: ValidationInputs,
        expires_at: datetime | None = None,
    ) -> ValidationRun:
        """
        Validate data against a set of traits.

        Args:
            trait_srns: Traits to validate against
            inputs: The data to validate
            expires_at: Optional expiry time for ephemeral runs

        Returns:
            ValidationRun with results
        """
        # Create validation run
        run_srn = ValidationRunSRN(
            domain=self.node_domain,
            id=LocalId(str(uuid.uuid4())),
            version=None,
        )

        run = ValidationRun(
            srn=run_srn,
            trait_srns=trait_srns,
            status=RunStatus.PENDING,
            results=[],
            started_at=None,
            completed_at=None,
            expires_at=expires_at,
        )
        await self.run_repo.save(run)

        # Start validation
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        results: list[CheckResult] = []
        overall_failed = False

        for trait_srn in trait_srns:
            result = await self._validate_trait(trait_srn, inputs)
            results.append(result)

            if result.status in (CheckStatus.FAILED, CheckStatus.ERROR):
                overall_failed = True

        # Complete validation run
        run.results = results
        run.status = RunStatus.FAILED if overall_failed else RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        return run

    async def _validate_trait(
        self,
        trait_srn: TraitSRN,
        inputs: ValidationInputs,
    ) -> CheckResult:
        """Validate inputs against a single trait."""
        try:
            trait = await self.trait_repo.get_or_fetch(trait_srn)
            validator = trait.validator

            output = await self.runner.run(
                image=validator.ref.image,
                digest=validator.ref.digest,
                inputs=inputs,
                timeout=validator.limits.timeout_seconds,
                resources=ResourceLimits(
                    memory=validator.limits.memory,
                    cpu=validator.limits.cpu,
                ),
            )

            return CheckResult(
                trait_srn=str(trait_srn),
                validator_digest=validator.ref.digest,
                status=output.status,
                message=output.error,
                details={"checks": output.checks} if output.checks else None,
            )

        except Exception as e:
            return CheckResult(
                trait_srn=str(trait_srn),
                validator_digest="",
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
