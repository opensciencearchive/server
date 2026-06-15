"""Validation service — orchestrates hook execution for depositions."""

import logging
import uuid
from datetime import datetime, timezone
from uuid import uuid4

from typing import Any

from osa.domain.shared.error import InfrastructureError, NotFoundError
from osa.domain.shared.model.hook import HookIdentity, HookName
from osa.domain.shared.model.srn import (
    ConventionSlug,
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
    ) -> tuple[ValidationRun, list[HookResult]]:
        """Execute hooks sequentially with OOM retry. Halt on reject/fail/OOM.

        Resolves each hook's live release once at run start (snapshot, R8) and
        records an append-only ``hook_run`` per hook (deposition context). The
        run ids are reconstructed at feature-insert time from the deposition,
        so they are recorded here but not returned.
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

        hook_results: list[HookResult] = []
        overall_status: RunStatus = RunStatus.COMPLETED

        for hook, release in pairs:
            work_dir = self.hook_storage.get_hook_output_dir(deposition_srn, hook.name.root)
            started_at = datetime.now(timezone.utc)
            run_id = HookRunId(uuid4())
            try:
                result = await hook_service.run_hook(hook, release, inputs, work_dir)
            except Exception as exc:
                finished_at = datetime.now(timezone.utc)
                # Record ANY failure as a terminal ERROR run. Capture the failed
                # container's logs (typed on InfrastructureError, else the message)
                # to a tenant-scoped artifact so the ERROR is diagnosable (#145/#147).
                text = (
                    exc.container_logs
                    if isinstance(exc, InfrastructureError) and exc.container_logs
                    else str(exc)
                )
                log_ref = await self.hook_storage.write_hook_log(work_dir, text)
                await self.hook_registry.record_run(
                    HookRun(
                        id=run_id,
                        release_id=release.id,
                        status=HookRunStatus.ERROR,
                        started_at=started_at,
                        finished_at=finished_at,
                        duration_s=(finished_at - started_at).total_seconds(),
                        # The run raised (no HookResult), so the retry count isn't
                        # available on this failure path; diagnose via log_ref.
                        oom_retries=0,
                        log_ref=log_ref,
                    )
                )
                overall_status = RunStatus.FAILED
                break

            finished_at = datetime.now(timezone.utc)
            # run.json carries run_id to InsertRecordFeatures — no DB lookup (§6).
            await self.hook_storage.write_run_ref(work_dir, str(run_id), str(release.id))
            await self.hook_registry.record_run(
                HookRun(
                    id=run_id,
                    release_id=release.id,
                    status=HookRunStatus.from_hook_status(result.status),
                    started_at=started_at,
                    finished_at=finished_at,
                    duration_s=result.duration_seconds,
                    oom_retries=result.oom_retries,
                )
            )
            hook_results.append(result)

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
        convention_id: ConventionSlug,
        metadata: dict[str, Any],
        hooks: list[HookName],
    ) -> tuple[ValidationRun, list[HookResult]]:
        """Full validation workflow using enriched event data.

        Uses the unified batch contract: constructs a 1-record batch for depositions.
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
            return run, []

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
