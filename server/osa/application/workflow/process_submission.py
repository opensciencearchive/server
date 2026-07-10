"""ProcessSubmission — orchestrates the deposition pipeline as stages (#160).

This handler replaces the five-hop event choreography
Validate → AutoApprove → Convert → InsertFeatures (plus ReturnToDraft) with a
single orchestrator. The outbox delivery is the durable job; the persisted
``SubmissionStage`` on the deposition is the restart position. On retry the
handler re-enters from the top and fast-forwards past stages the checkpoint has
already cleared, so a failure after validation never re-runs validation
containers.

Two deliberate behavioural improvements over the choreography it replaces:

* A ``ValueError`` raised while setting up validation is converted to a
  ``PermanentError`` (dead-letter). Today's ``ValidateDeposition`` swallows it,
  leaving the deposition stuck ``IN_VALIDATION`` forever.
* The success path now sets ``record_srn`` + ``status = ACCEPTED`` (decision 11
  of the #160 plan). Today's success path leaves the deposition
  ``IN_VALIDATION`` with ``record_srn`` unset forever.
"""

from typing import ClassVar
from uuid import uuid4

from osa.application.workflow.stages import StageRunner
from osa.domain.curation.event.deposition_approved import DepositionApproved
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionStatus, SubmissionStage
from osa.domain.deposition.port.repository import DepositionRepository
from osa.domain.feature.port.storage import FeatureStoragePort
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.model.draft import RecordDraft
from osa.domain.record.service.record import RecordService
from osa.domain.shared.error import InvalidStateError, NotFoundError, PermanentError
from osa.domain.shared.event import EventHandler, EventId
from osa.domain.shared.model.hook import FeatureName
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.workflow import WorkflowName, WorkflowStage
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.port.instrumentation import WorkflowInstrumentation
from osa.domain.shared.port.unit_of_work import UnitOfWork
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.event.validation_failed import ValidationFailed
from osa.domain.validation.model import RunStatus
from osa.domain.validation.service.validation import ValidationService
from osa.infrastructure.logging import get_logger

log = get_logger(__name__)

_TERMINAL_VALIDATION = (RunStatus.FAILED, RunStatus.REJECTED)


class ProcessSubmission(EventHandler[DepositionSubmittedEvent]):
    """Orchestrates the whole deposition pipeline as sequential stages (#160).

    Replaces the five-hop choreography Validate→AutoApprove→Convert→InsertFeatures
    (+ReturnToDraft). The outbox delivery is the durable job; the persisted
    ``SubmissionStage`` checkpoint is the restart position: on retry the handler
    re-enters from the top and fast-forwards past completed stages, so a failure
    after validation never re-runs validation containers.
    """

    __claim_timeout__: ClassVar[float] = 3600.0  # validation runs containers
    __max_retries__: ClassVar[int] = 10  # transient budget; permanent errors bypass it

    deposition_repo: DepositionRepository
    validation_service: ValidationService
    record_service: RecordService
    feature_service: FeatureService
    feature_storage: FeatureStoragePort
    outbox: Outbox
    uow: UnitOfWork
    instrumentation: WorkflowInstrumentation

    async def handle(self, event: DepositionSubmittedEvent) -> None:
        """Drive VALIDATE → CURATE → PUBLISH → INSERT_FEATURES to completion."""
        dep = await self.deposition_repo.get(event.deposition_id)
        if dep is None:
            # Deterministic: this delivery can never succeed — dead-letter now
            # rather than burn a container-sized retry budget on a missing row.
            raise PermanentError(f"Deposition not found: {event.deposition_id}") from NotFoundError(
                f"Deposition not found: {event.deposition_id}"
            )

        # The publish stage commits ACCEPTED before the feature stage runs, so a
        # crash or error during feature insertion redelivers with an ACCEPTED
        # deposition. That delivery must still be admitted — the stage guards
        # skip straight to the (idempotent) feature stage — or features written
        # by no other path would be silently lost.
        resuming_after_publish = (
            dep.status == DepositionStatus.ACCEPTED and dep.stage >= SubmissionStage.PUBLISHED
        )
        if dep.status != DepositionStatus.IN_VALIDATION and not resuming_after_publish:
            log.warn(
                "submission skipped — deposition is {status}, not in_validation",
                status=dep.status.value,
                deposition_id=str(event.deposition_id),
            )
            return

        # Make the claim durable before parking on long container awaits.
        await self.uow.commit()

        runner = StageRunner(WorkflowName.PROCESS_SUBMISSION, self.instrumentation)

        if dep.stage < SubmissionStage.VALIDATED:
            proceed = await self._validate(event, dep, runner)
            if not proceed:
                return
        else:
            runner.skipped(WorkflowStage.VALIDATE)
            runner.skipped(WorkflowStage.CURATE)

        if dep.stage < SubmissionStage.PUBLISHED:
            await self._publish(event, dep, runner)
        else:
            runner.skipped(WorkflowStage.PUBLISH)
            if dep.record_srn is None:
                # Invariant violation: the checkpoint says published but no
                # record SRN was ever written — non-recoverable.
                raise PermanentError(
                    f"Deposition {event.deposition_id} at stage PUBLISHED with no record_srn"
                )

        await self._insert_features(event, dep, runner)

    async def _validate(
        self, event: DepositionSubmittedEvent, dep: Deposition, runner: StageRunner
    ) -> bool:
        """Run validation + the auto-approve curation gate.

        Returns ``True`` to proceed to publication, ``False`` when validation
        reached a FAILED/REJECTED verdict and the deposition was returned to
        draft. A raised exception (setup ``ValueError`` → ``PermanentError``)
        propagates and marks the stage FAILED — the terminal verdicts do not.
        """
        async with runner.run(WorkflowStage.VALIDATE):
            try:
                run, hook_results = await self.validation_service.validate_deposition(
                    deposition_srn=event.deposition_id,
                    convention_id=event.convention_id,
                    metadata=event.metadata,
                    hooks=event.hooks,
                )
            except ValueError as exc:
                # Unlike today's ValidateDeposition (which swallows this, leaving
                # the deposition stuck IN_VALIDATION), dead-letter deterministically.
                raise PermanentError(
                    f"Validation setup failed for {event.deposition_id}: {exc}"
                ) from exc

            if run.status in _TERMINAL_VALIDATION:
                reasons = [
                    r.error_message or r.rejection_reason or "Unknown"
                    for r in hook_results
                    if r.error_message or r.rejection_reason
                ]
                await self.outbox.append(
                    ValidationFailed(
                        id=EventId(uuid4()),
                        deposition_srn=event.deposition_id,
                        convention_id=event.convention_id,
                        status=run.status,
                        reasons=reasons,
                    )
                )
                dep.return_to_draft()
                await self.deposition_repo.save(dep)
                # Validation ran to a verdict — the stage records RAN; the
                # scope-exit commit makes this atomic with the delivery mark.
                return False

            expected_features = [FeatureName(h.root) for h in event.hooks]
            await self.outbox.append(
                ValidationCompleted(
                    id=EventId(uuid4()),
                    validation_run_srn=run.srn,
                    deposition_srn=event.deposition_id,
                    convention_id=event.convention_id,
                    status=run.status,
                    hook_results=[r.model_dump() for r in hook_results],
                    metadata=event.metadata,
                    expected_features=expected_features,
                )
            )

        # CURATE gate — v1 auto-approves (curation_required = False, matching
        # today's AutoApproveCuration). Same transaction as validation.
        async with runner.run(WorkflowStage.CURATE):
            await self.outbox.append(
                DepositionApproved(
                    id=EventId(uuid4()),
                    deposition_srn=event.deposition_id,
                    metadata=event.metadata,
                    convention_id=event.convention_id,
                    expected_features=[FeatureName(h.root) for h in event.hooks],
                )
            )

        dep.stage = SubmissionStage.VALIDATED
        await self.deposition_repo.save(dep)
        # Checkpoint: after this commit a retry fast-forwards past VALIDATE, so
        # validation containers never re-run.
        await self.uow.commit()
        return True

    async def _publish(
        self, event: DepositionSubmittedEvent, dep: Deposition, runner: StageRunner
    ) -> None:
        """Publish the record and complete the deposition."""
        async with runner.run(WorkflowStage.PUBLISH):
            draft = RecordDraft(
                source=DepositionSource(id=str(event.deposition_id)),
                metadata=event.metadata,
                convention_id=event.convention_id,
                expected_features=[FeatureName(h.root) for h in event.hooks],
            )
            try:
                record = await self.record_service.publish_record(draft)
            except NotFoundError as exc:
                raise PermanentError(f"Cannot publish {event.deposition_id}: {exc}") from exc

            # Complete the deposition — sets record_srn + ACCEPTED, closing the
            # audited gap where the success path left it IN_VALIDATION forever.
            dep.record_srn = record.srn
            dep.status = DepositionStatus.ACCEPTED
            dep.stage = SubmissionStage.PUBLISHED
            await self.deposition_repo.save(dep)

        # Records must be committed before feature inserts: the feature store
        # writes on a separate engine with an FK to records.
        await self.uow.commit()

    async def _insert_features(
        self, event: DepositionSubmittedEvent, dep: Deposition, runner: StageRunner
    ) -> None:
        """Insert this record's hook outputs (harmless to redo — replace semantics)."""
        async with runner.run(WorkflowStage.INSERT_FEATURES):
            if dep.record_srn is None:
                raise PermanentError(
                    f"Deposition {event.deposition_id} reached INSERT_FEATURES with no record_srn"
                )
            source = DepositionSource(id=str(event.deposition_id))
            hook_output_dir = self.feature_storage.get_hook_output_root(source.type, source.id)
            await self.feature_service.insert_features_for_record(
                hook_output_dir=hook_output_dir,
                record_srn=str(dep.record_srn),
                expected_features=[FeatureName(h.root) for h in event.hooks],
            )

    async def on_exhausted(self, event: DepositionSubmittedEvent) -> None:
        """Best-effort recovery once the transient retry budget is spent."""
        dep = await self.deposition_repo.get(event.deposition_id)
        if dep is None:
            log.error(
                "submission retries exhausted for a missing deposition",
                deposition_id=str(event.deposition_id),
            )
            return

        if dep.status == DepositionStatus.IN_VALIDATION and dep.stage < SubmissionStage.PUBLISHED:
            try:
                dep.return_to_draft()
            except InvalidStateError as exc:
                log.warn(
                    "could not return exhausted deposition to draft: {reason}",
                    reason=str(exc),
                    deposition_id=str(event.deposition_id),
                )
                return
            await self.deposition_repo.save(dep)
            await self.outbox.append(
                ValidationFailed(
                    id=EventId(uuid4()),
                    deposition_srn=event.deposition_id,
                    convention_id=event.convention_id,
                    status=RunStatus.FAILED,
                    reasons=["submission processing retries exhausted"],
                )
            )
        elif dep.stage >= SubmissionStage.PUBLISHED:
            # The record exists; only feature insertion may be incomplete. That
            # is visible operationally — nothing to roll back.
            log.error(
                "submission retries exhausted after publish; features may be incomplete",
                deposition_id=str(event.deposition_id),
                record_srn=str(dep.record_srn),
            )
