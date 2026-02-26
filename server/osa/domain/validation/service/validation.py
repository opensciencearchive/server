"""Validation service — orchestrates hook execution for depositions."""

import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from osa.domain.shared.model.hook import (
    FeatureSchema,
    HookDefinition,
    HookManifest,
)
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import (
    ConventionSRN,
    DepositionSRN,
    Domain,
    LocalId,
    ValidationRunSRN,
)
from osa.domain.shared.service import Service
from osa.domain.validation.model import (
    RunStatus,
    ValidationRun,
)
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.domain.validation.port.storage import HookStoragePort

logger = logging.getLogger(__name__)


def _snapshot_to_hook_definition(snapshot: HookSnapshot) -> HookDefinition:
    """Convert a HookSnapshot to a HookDefinition for the runner."""
    return HookDefinition(
        image=snapshot.image,
        digest=snapshot.digest,
        config=snapshot.config or None,
        manifest=HookManifest(
            name=snapshot.name,
            record_schema="",
            cardinality="one",
            feature_schema=FeatureSchema(columns=snapshot.features),
        ),
    )


class ValidationService(Service):
    """Orchestrates hook execution for depositions."""

    run_repo: ValidationRunRepository
    hook_runner: HookRunner
    hook_storage: HookStoragePort
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
        hooks: list[HookSnapshot],
    ) -> tuple[ValidationRun, list[HookResult]]:
        """Execute hooks sequentially. Halt on reject/fail.

        Hook outputs are written to durable cold storage under the deposition directory.
        Feature insertion is deferred to record publication time.
        """
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        hook_results: list[HookResult] = []
        overall_status: RunStatus = RunStatus.COMPLETED

        for hook_snapshot in hooks:
            work_dir = self.hook_storage.get_hook_output_dir(deposition_srn, hook_snapshot.name)
            hook_def = _snapshot_to_hook_definition(hook_snapshot)
            result = await self.hook_runner.run(hook_def, inputs, work_dir)
            hook_results.append(result)

            if result.status == HookStatus.FAILED:
                overall_status = RunStatus.FAILED
                break
            if result.status == HookStatus.REJECTED:
                overall_status = RunStatus.REJECTED
                break

        run.results = hook_results
        run.status = overall_status
        run.completed_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        return run, hook_results

    async def validate_deposition(
        self,
        deposition_srn: DepositionSRN,
        convention_srn: ConventionSRN,
        metadata: dict[str, Any],
        hooks: list[HookSnapshot],
        files_dir: str,
    ) -> tuple[ValidationRun, list[HookResult]]:
        """Full validation workflow using enriched event data."""
        record_json = {"srn": str(deposition_srn), "metadata": metadata}
        inputs = HookInputs(
            record_json=record_json,
            files_dir=Path(files_dir) if files_dir else None,
        )

        run = await self.create_run(inputs=inputs)

        if not hooks:
            logger.debug("No hooks configured, instant pass")
            run.status = RunStatus.COMPLETED
            await self.run_repo.save(run)
            return run, []

        run, hook_results = await self.run_hooks(
            run=run,
            deposition_srn=deposition_srn,
            inputs=inputs,
            hooks=hooks,
        )

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
