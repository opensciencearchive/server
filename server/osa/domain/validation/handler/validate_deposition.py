"""ValidateDeposition - handles DepositionSubmitted events."""

import logging
from uuid import uuid4

from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.port.convention_repository import ConventionRepository
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.deposition.port.storage import FileStoragePort
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.outbox import Outbox
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.event.validation_failed import ValidationFailed
from osa.domain.validation.model import RunStatus
from osa.domain.validation.port.hook_runner import HookInputs
from osa.domain.validation.service.validation import ValidationService

logger = logging.getLogger(__name__)


class ValidateDeposition(EventHandler[DepositionSubmittedEvent]):
    """Runs hooks on depositions. 0 hooks = instant pass."""

    outbox: Outbox
    deposition_repo: DepositionRepository
    convention_repo: ConventionRepository
    file_storage: FileStoragePort
    validation_service: ValidationService

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        """Run hooks and emit ValidationCompleted or ValidationFailed."""
        logger.debug(f"Validating deposition: {event.deposition_id}")

        dep = await self.deposition_repo.get(event.deposition_id)
        if dep is None:
            logger.error(f"Deposition not found: {event.deposition_id}")
            return

        convention = await self.convention_repo.get(dep.convention_srn)
        if convention is None:
            logger.error(f"Convention not found: {dep.convention_srn}")
            return

        # Create validation run
        run = await self.validation_service.create_run(
            inputs=HookInputs(
                record_json=dep.metadata,
                files_dir=None,
            ),
        )

        if not convention.hooks:
            logger.debug("No hooks configured, instant pass")
            run.status = RunStatus.COMPLETED
            await self.validation_service.save_run(run)

            completed = ValidationCompleted(
                id=EventId(uuid4()),
                validation_run_srn=run.srn,
                deposition_srn=event.deposition_id,
                convention_srn=dep.convention_srn,
                status=RunStatus.COMPLETED,
                hook_results=[],
                metadata=event.metadata,
            )
            await self.outbox.append(completed)
            return

        # Run hooks via the validation service
        inputs = HookInputs(
            record_json=dep.metadata,
            files_dir=None,
        )
        convention_id = str(convention.srn.id)
        run, hook_results = await self.validation_service.run_hooks(
            run=run,
            convention_id=convention_id,
            inputs=inputs,
            hooks=convention.hooks,
        )

        if run.status == RunStatus.FAILED:
            reasons = [
                r.error_message or r.rejection_reason or "Unknown"
                for r in hook_results
                if r.error_message or r.rejection_reason
            ]
            failed = ValidationFailed(
                id=EventId(uuid4()),
                deposition_srn=event.deposition_id,
                convention_srn=dep.convention_srn,
                status=run.status,
                reasons=reasons,
            )
            await self.outbox.append(failed)
            logger.info(f"Validation failed for: {event.deposition_id}")
        else:
            completed = ValidationCompleted(
                id=EventId(uuid4()),
                validation_run_srn=run.srn,
                deposition_srn=event.deposition_id,
                convention_srn=dep.convention_srn,
                status=run.status,
                hook_results=[r.model_dump() for r in hook_results],
                metadata=event.metadata,
            )
            await self.outbox.append(completed)
            logger.debug(f"Validation completed for: {event.deposition_id}")
