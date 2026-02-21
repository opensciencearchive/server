"""Validation service — orchestrates hook execution for depositions."""

import shutil
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from osa.domain.feature.service.feature import FeatureService
from osa.domain.shared.model.hook import HookDefinition
from osa.domain.shared.model.srn import Domain, LocalId, ValidationRunSRN
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
    feature_service: FeatureService
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
        convention_id: str,
        inputs: HookInputs,
        hooks: list[HookDefinition],
    ) -> tuple[ValidationRun, list[HookResult]]:
        """Execute hooks sequentially. Halt on reject/fail."""
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        hook_results: list[HookResult] = []
        overall_failed = False

        for hook_def in hooks:
            workspace_dir = Path(tempfile.mkdtemp())
            try:
                result = await self.hook_runner.run(hook_def, inputs, workspace_dir)
                hook_results.append(result)

                if result.status == HookStatus.REJECTED:
                    overall_failed = True
                    break
                elif result.status == HookStatus.FAILED:
                    overall_failed = True
                    break

                # Insert features on success
                if result.features:
                    record_srn = inputs.record_json.get("srn")
                    if not record_srn:
                        raise ValueError("record_json missing required 'srn' field")
                    await self.feature_service.insert_features(
                        convention_id=convention_id,
                        hook_name=result.hook_name,
                        record_srn=record_srn,
                        rows=result.features,
                    )
            finally:
                shutil.rmtree(workspace_dir, ignore_errors=True)

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
