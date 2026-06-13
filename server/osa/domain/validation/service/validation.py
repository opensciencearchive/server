"""Validation service — orchestrates hook execution for depositions."""

import logging
import uuid
from datetime import datetime, timezone
from uuid import uuid4

from typing import Any

from osa.domain.shared.error import NotFoundError
from osa.domain.shared.model.hook import HookIdentity, HookName
from osa.domain.shared.model.srn import (
    ConventionId,
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
from osa.domain.validation.model.hook_input import HookRecord
from osa.domain.validation.model.hook_release import HookRelease
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.model.hook_run import HookRun, HookRunId, HookRunStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner
from osa.domain.validation.port.repository import ValidationRunRepository
from osa.domain.validation.port.storage import HookStoragePort
from osa.domain.validation.service.hook import HookService
from osa.domain.validation.service.hook_registry import HookRegistryService

logger = logging.getLogger(__name__)


def _run_status(hook_status: HookStatus | None) -> HookRunStatus:
    if hook_status == HookStatus.PASSED:
        return HookRunStatus.PASSED
    if hook_status == HookStatus.REJECTED:
        return HookRunStatus.FAILED
    return HookRunStatus.ERROR


class ValidationService(Service):
    """Orchestrates hook execution for depositions."""

    run_repo: ValidationRunRepository
    hook_runner: HookRunner
    hook_storage: HookStoragePort
    hook_registry: HookRegistryService
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
        hook_names: list[HookName],
    ) -> tuple[ValidationRun, list[HookResult], dict[str, str]]:
        """Execute hooks sequentially with OOM retry. Halt on reject/fail/OOM.

        Resolves each hook's live release once at run start (snapshot, R8),
        records an append-only ``hook_run`` per hook (deposition context), and
        returns ``{hook_name: hook_run_id}`` so feature rows can be stamped with
        their provenance at publication time.
        """
        run.status = RunStatus.RUNNING
        run.started_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        hook_service = HookService(
            hook_runner=self.hook_runner,
            hook_storage=self.hook_storage,
        )

        # Resolve identity + live release per hook (snapshot).
        releases = await self.hook_registry.resolve_live(hook_names)
        pairs: list[tuple[HookIdentity, HookRelease]] = []
        for name in hook_names:
            hook = await self.hook_registry.get_hook(name)
            release = releases.get(name)
            if hook is None or release is None:
                raise NotFoundError(f"Hook {name!r} has no live release")
            pairs.append((HookIdentity(name=hook.name, feature=hook.feature), release))

        run_id_by_hook: dict[str, str] = {}
        hook_results: list[HookResult] = []
        overall_status: RunStatus = RunStatus.COMPLETED

        for hook, release in pairs:
            work_dir = self.hook_storage.get_hook_output_dir(deposition_srn, hook.name)
            started_at = datetime.now(timezone.utc)
            run_id = HookRunId(uuid4())
            try:
                result = await hook_service.run_hook(hook, release, inputs, work_dir)
            except Exception:
                await self.hook_registry.record_run(
                    HookRun(
                        id=run_id,
                        release_id=release.id,
                        deposition_id=deposition_srn,
                        status=HookRunStatus.ERROR,
                        started_at=started_at,
                        finished_at=datetime.now(timezone.utc),
                    )
                )
                run_id_by_hook[hook.name] = str(run_id)
                overall_status = RunStatus.FAILED
                break

            await self.hook_registry.record_run(
                HookRun(
                    id=run_id,
                    release_id=release.id,
                    deposition_id=deposition_srn,
                    status=_run_status(result.status),
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc),
                    duration_s=result.duration_seconds,
                )
            )
            run_id_by_hook[hook.name] = str(run_id)
            hook_results.append(result)

            if result.status == HookStatus.REJECTED:
                overall_status = RunStatus.REJECTED
                break

        run.results = hook_results
        run.status = overall_status
        run.completed_at = datetime.now(timezone.utc)
        await self.run_repo.save(run)

        return run, hook_results, run_id_by_hook

    async def validate_deposition(
        self,
        deposition_srn: DepositionSRN,
        convention_id: ConventionId,
        metadata: dict[str, Any],
        hooks: list[HookName],
    ) -> tuple[ValidationRun, list[HookResult], dict[str, str]]:
        """Full validation workflow using enriched event data.

        Uses the unified batch contract: constructs a 1-record batch for depositions.
        Returns the run, per-hook results, and ``{hook_name: hook_run_id}``.
        """
        local_id = deposition_srn.id.root
        record = HookRecord(id=local_id, metadata=metadata)
        run_id = f"{deposition_srn.domain.root}_{local_id}"
        files_dir = self.hook_storage.get_files_dir(deposition_srn)

        inputs = HookInputs(
            records=[record],
            run_id=run_id,
            files_dirs={local_id: files_dir} if files_dir else {},
        )

        run = await self.create_run(inputs=inputs)

        if not hooks:
            logger.debug("No hooks configured, instant pass")
            run.status = RunStatus.COMPLETED
            await self.run_repo.save(run)
            return run, [], {}

        return await self.run_hooks(
            run=run,
            deposition_srn=deposition_srn,
            inputs=inputs,
            hook_names=hooks,
        )

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
