"""Validation service — orchestrates hook execution for depositions."""

import uuid
from datetime import datetime, timezone

from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import DepositionSRN, Domain, LocalId, ValidationRunSRN
from osa.domain.shared.service import Service
from osa.domain.validation.model import (
    RunStatus,
    ValidationRun,
)
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.domain.validation.port.repository import ValidationRunRepository


class ValidationService(Service):
    """Orchestrates hook execution for depositions."""

    run_repo: ValidationRunRepository
    hook_runner: HookRunner
    file_storage: FileStoragePort
    node_domain: Domain

    async def create_run(
        self,
        inputs: HookInputs,
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

    async def run_hooks(
        self,
        run: ValidationRun,
        deposition_srn: DepositionSRN,
        inputs: HookInputs,
        hooks: list[HookDefinition],
    ) -> tuple[ValidationRun, list[HookResult]]:
        """Execute hooks sequentially. Halt on reject/fail.

        Hook outputs are written to durable cold storage under the deposition directory.
        Feature insertion is deferred to record publication time.
        """
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        hook_results: list[HookResult] = []
        overall_failed = False

        for hook_def in hooks:
            work_dir = self.file_storage.get_hook_output_dir(deposition_srn, hook_def.manifest.name)
            result = await self.hook_runner.run(hook_def, inputs, work_dir)
            hook_results.append(result)

            if result.status in (HookStatus.REJECTED, HookStatus.FAILED):
                overall_failed = True
                break

        run.results = hook_results
        run.status = RunStatus.FAILED if overall_failed else RunStatus.COMPLETED
        run.completed_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        return run, hook_results

    async def save_run(self, run: ValidationRun) -> None:
        """Persist a validation run."""
        await self.run_repo.save(run)

    async def get_run(self, run_id: str) -> ValidationRun | None:
        """Get a validation run by its ID (local part of SRN)."""
        run_srn = ValidationRunSRN(
            domain=self.node_domain,
            id=LocalId(run_id),
            version=None,
        )
        return await self.run_repo.get(run_srn)
