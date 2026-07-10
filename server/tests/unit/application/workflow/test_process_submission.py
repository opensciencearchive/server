"""Unit tests for :class:`ProcessSubmission` (#160).

ProcessSubmission collapses the five-hop deposition choreography
(Validate → AutoApprove → Convert → InsertFeatures, plus ReturnToDraft) into a
single orchestrated handler driven by the persisted ``SubmissionStage``
checkpoint. These tests pin the absorbed event payloads, the stage-outcome
emissions, and — critically — the commit checkpoint ordering that guarantees a
retry never re-runs validation containers.

Fakes over mocks where ordering matters: a shared ``order`` log threads through
the unit-of-work, services, and outbox so cross-object call ordering is
asserted directly. A recording instrumentation fake stands in for the
``WorkflowInstrumentation`` port.
"""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock
from uuid import uuid4

import pytest

from osa.application.workflow.process_submission import ProcessSubmission
from osa.domain.auth.model.value import UserId
from osa.domain.curation.event.deposition_approved import DepositionApproved
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionStatus, SubmissionStage
from osa.domain.record.model.draft import RecordDraft
from osa.domain.shared.error import PermanentError
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import FeatureName, HookName
from osa.domain.shared.model.source import DepositionSource
from osa.domain.shared.model.srn import (
    ConventionSlug,
    DepositionSRN,
    RecordSRN,
    ValidationRunSRN,
)
from osa.domain.shared.model.workflow import StageOutcome, WorkflowName, WorkflowStage
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.event.validation_failed import ValidationFailed
from osa.domain.validation.model import RunStatus
from osa.domain.validation.model.hook_result import HookResult, HookStatus

_PS = WorkflowName.PROCESS_SUBMISSION


def _dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _conv() -> ConventionSlug:
    return ConventionSlug("test-conv")


def _record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


def _hook() -> HookName:
    return HookName("pocket_detect")


def _make_deposition(
    *,
    status: DepositionStatus = DepositionStatus.IN_VALIDATION,
    stage: SubmissionStage = SubmissionStage.SUBMITTED,
    record_srn: RecordSRN | None = None,
) -> Deposition:
    return Deposition(
        srn=_dep_srn(),
        convention_id=_conv(),
        status=status,
        stage=stage,
        record_srn=record_srn,
        owner_id=UserId(uuid4()),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _event(hooks: list[HookName] | None = None) -> DepositionSubmittedEvent:
    return DepositionSubmittedEvent(
        id=EventId(uuid4()),
        deposition_id=_dep_srn(),
        metadata={"title": "Test"},
        convention_id=_conv(),
        hooks=hooks if hooks is not None else [_hook()],
    )


@dataclass
class _RecordingInstrumentation:
    calls: list[tuple[WorkflowName, WorkflowStage, StageOutcome]] = field(default_factory=list)

    def stage_finished(
        self, *, workflow: WorkflowName, stage: WorkflowStage, outcome: StageOutcome
    ) -> None:
        self.calls.append((workflow, stage, outcome))


def _build(
    dep: Deposition | None,
    *,
    run_status: RunStatus = RunStatus.COMPLETED,
    hook_results: list[HookResult] | None = None,
    validate_error: Exception | None = None,
) -> tuple[ProcessSubmission, SimpleNamespace]:
    """Wire a handler with fakes sharing a single ``order`` log."""
    order: list[str] = []

    deposition_service = AsyncMock()

    async def _get(_srn: object) -> Deposition:
        if dep is None:
            from osa.domain.shared.error import NotFoundError

            raise NotFoundError("Deposition not found")
        return dep

    deposition_service.get.side_effect = _get

    async def _return_to_draft(_srn: object) -> Deposition:
        assert dep is not None
        dep.return_to_draft()
        order.append("save")
        return dep

    deposition_service.return_to_draft.side_effect = _return_to_draft

    async def _mark_validated(_srn: object) -> Deposition:
        assert dep is not None
        dep.mark_validated()
        order.append("save")
        return dep

    deposition_service.mark_validated.side_effect = _mark_validated

    async def _accept(_srn: object, *, record_srn: RecordSRN) -> Deposition:
        assert dep is not None
        dep.accept(record_srn)
        order.append("save")
        return dep

    deposition_service.accept.side_effect = _accept

    run_mock = MagicMock()
    run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
    run_mock.status = run_status
    validation_service = AsyncMock()

    async def _validate(**_kwargs: object) -> tuple[MagicMock, list[HookResult]]:
        order.append("validate")
        if validate_error is not None:
            raise validate_error
        return run_mock, hook_results or []

    validation_service.validate_deposition.side_effect = _validate

    rec = MagicMock()
    rec.srn = _record_srn()
    record_service = AsyncMock()

    async def _publish(_draft: RecordDraft) -> MagicMock:
        order.append("publish")
        return rec

    record_service.publish_record.side_effect = _publish

    feature_service = AsyncMock()

    async def _insert(**_kwargs: object) -> None:
        order.append("insert_features")

    feature_service.insert_features_for_record.side_effect = _insert

    feature_storage = Mock()
    feature_storage.get_hook_output_root.return_value = "/fake/out"

    appended: list[object] = []
    outbox = AsyncMock()

    async def _append(evt: object, **_kwargs: object) -> None:
        appended.append(evt)
        order.append(f"append:{type(evt).__name__}")

    outbox.append.side_effect = _append

    uow = AsyncMock()

    async def _commit() -> None:
        order.append("commit")

    uow.commit.side_effect = _commit

    instr = _RecordingInstrumentation()

    handler = ProcessSubmission(
        deposition_service=deposition_service,
        validation_service=validation_service,
        record_service=record_service,
        feature_service=feature_service,
        feature_storage=feature_storage,
        outbox=outbox,
        uow=uow,
        instrumentation=instr,
    )
    bag = SimpleNamespace(
        order=order,
        appended=appended,
        rec=rec,
        deposition_service=deposition_service,
        validation_service=validation_service,
        record_service=record_service,
        feature_service=feature_service,
        feature_storage=feature_storage,
        outbox=outbox,
        uow=uow,
        instr=instr,
    )
    return handler, bag


async def test_happy_path_events_state_and_features() -> None:
    dep = _make_deposition()
    handler, bag = _build(dep)

    await handler.handle(_event())

    # validation delegated with the event's enriched data
    bag.validation_service.validate_deposition.assert_called_once_with(
        deposition_srn=_dep_srn(),
        convention_id=_conv(),
        metadata={"title": "Test"},
        hooks=[_hook()],
    )

    # our outbox carries exactly ValidationCompleted then DepositionApproved;
    # RecordPublished is the record service's responsibility (mocked away).
    types = [type(e).__name__ for e in bag.appended]
    assert types == ["ValidationCompleted", "DepositionApproved"]
    completed = bag.appended[0]
    assert isinstance(completed, ValidationCompleted)
    assert completed.validation_run_srn == ValidationRunSRN.parse("urn:osa:localhost:val:run1")
    assert completed.status == RunStatus.COMPLETED
    assert completed.expected_features == [FeatureName("pocket_detect")]
    approved = bag.appended[1]
    assert isinstance(approved, DepositionApproved)
    assert approved.expected_features == [FeatureName("pocket_detect")]

    # publish called with a faithful RecordDraft
    bag.record_service.publish_record.assert_called_once()
    draft = bag.record_service.publish_record.call_args[0][0]
    assert isinstance(draft, RecordDraft)
    assert isinstance(draft.source, DepositionSource)
    assert draft.source.id == str(_dep_srn())
    assert draft.convention_id == _conv()
    assert draft.metadata == {"title": "Test"}
    assert [f.root for f in draft.expected_features] == ["pocket_detect"]

    # deposition ends fully accepted + published, pointing at the record
    assert dep.stage == SubmissionStage.PUBLISHED
    assert dep.status == DepositionStatus.ACCEPTED
    assert dep.record_srn == bag.rec.srn

    # features inserted for the freshly published record
    bag.feature_storage.get_hook_output_root.assert_called_once_with("deposition", str(_dep_srn()))
    bag.feature_service.insert_features_for_record.assert_called_once_with(
        hook_output_dir="/fake/out",
        record_srn=str(bag.rec.srn),
        expected_features=[FeatureName("pocket_detect")],
    )


async def test_checkpoint_ordering() -> None:
    dep = _make_deposition()
    handler, bag = _build(dep)

    await handler.handle(_event())

    assert bag.order == [
        "commit",  # claim durable, before any work
        "validate",
        "append:ValidationCompleted",
        "append:DepositionApproved",
        "save",  # stage -> VALIDATED
        "commit",  # checkpoint: containers never re-run past here
        "publish",
        "save",  # stage -> PUBLISHED, status ACCEPTED, record_srn set
        "commit",  # records committed before features (separate engine, FK)
        "insert_features",  # rides scope-exit commit; NO commit follows
    ]
    assert bag.order.count("commit") == 3


@pytest.mark.parametrize("run_status", [RunStatus.FAILED, RunStatus.REJECTED])
async def test_validation_failed_returns_to_draft(run_status: RunStatus) -> None:
    dep = _make_deposition()
    result = HookResult(
        hook_name="pocket_detect",
        status=HookStatus.REJECTED,
        rejection_reason="data quality too low",
        duration_seconds=1.0,
    )
    handler, bag = _build(dep, run_status=run_status, hook_results=[result])

    await handler.handle(_event())

    types = [type(e).__name__ for e in bag.appended]
    assert types == ["ValidationFailed"]
    failed = bag.appended[0]
    assert isinstance(failed, ValidationFailed)
    assert failed.status == run_status
    assert failed.reasons == ["data quality too low"]

    assert dep.status == DepositionStatus.DRAFT
    bag.deposition_service.return_to_draft.assert_awaited()
    bag.record_service.publish_record.assert_not_called()
    bag.feature_service.insert_features_for_record.assert_not_called()
    # only the claim commit — the failure branch adds none
    assert bag.order.count("commit") == 1
    assert bag.instr.calls == [(_PS, WorkflowStage.VALIDATE, StageOutcome.RAN)]


async def test_fast_forward_from_validated_skips_validation() -> None:
    dep = _make_deposition(stage=SubmissionStage.VALIDATED)
    handler, bag = _build(dep)

    await handler.handle(_event())

    bag.validation_service.validate_deposition.assert_not_called()
    bag.record_service.publish_record.assert_called_once()
    bag.feature_service.insert_features_for_record.assert_called_once()
    assert dep.stage == SubmissionStage.PUBLISHED
    assert dep.status == DepositionStatus.ACCEPTED

    assert bag.instr.calls == [
        (_PS, WorkflowStage.VALIDATE, StageOutcome.SKIPPED),
        (_PS, WorkflowStage.CURATE, StageOutcome.SKIPPED),
        (_PS, WorkflowStage.PUBLISH, StageOutcome.RAN),
        (_PS, WorkflowStage.INSERT_FEATURES, StageOutcome.RAN),
    ]


async def test_fast_forward_from_published_only_inserts_features() -> None:
    # The realistic crash-during-features redelivery: publish committed
    # ACCEPTED + PUBLISHED, then feature insertion failed. The entry guard must
    # admit this delivery so the (idempotent) feature stage can redo —
    # otherwise the features would be silently lost.
    dep = _make_deposition(
        status=DepositionStatus.ACCEPTED,
        stage=SubmissionStage.PUBLISHED,
        record_srn=_record_srn(),
    )
    handler, bag = _build(dep)

    await handler.handle(_event())

    bag.validation_service.validate_deposition.assert_not_called()
    bag.record_service.publish_record.assert_not_called()
    bag.feature_service.insert_features_for_record.assert_called_once_with(
        hook_output_dir="/fake/out",
        record_srn=str(_record_srn()),
        expected_features=[FeatureName("pocket_detect")],
    )
    assert bag.instr.calls == [
        (_PS, WorkflowStage.VALIDATE, StageOutcome.SKIPPED),
        (_PS, WorkflowStage.CURATE, StageOutcome.SKIPPED),
        (_PS, WorkflowStage.PUBLISH, StageOutcome.SKIPPED),
        (_PS, WorkflowStage.INSERT_FEATURES, StageOutcome.RAN),
    ]


async def test_happy_path_emits_ran_for_all_four_stages() -> None:
    handler, bag = _build(_make_deposition())

    await handler.handle(_event())

    assert bag.instr.calls == [
        (_PS, WorkflowStage.VALIDATE, StageOutcome.RAN),
        (_PS, WorkflowStage.CURATE, StageOutcome.RAN),
        (_PS, WorkflowStage.PUBLISH, StageOutcome.RAN),
        (_PS, WorkflowStage.INSERT_FEATURES, StageOutcome.RAN),
    ]


async def test_published_stage_without_record_srn_raises_permanent() -> None:
    dep = _make_deposition(stage=SubmissionStage.PUBLISHED, record_srn=None)
    handler, bag = _build(dep)

    with pytest.raises(PermanentError):
        await handler.handle(_event())

    bag.feature_service.insert_features_for_record.assert_not_called()


async def test_missing_deposition_raises_permanent_chained() -> None:
    handler, bag = _build(None)

    with pytest.raises(PermanentError) as excinfo:
        await handler.handle(_event())

    from osa.domain.shared.error import NotFoundError

    assert isinstance(excinfo.value.__cause__, NotFoundError)
    bag.uow.commit.assert_not_called()


async def test_wrong_status_returns_without_touching_services() -> None:
    dep = _make_deposition(status=DepositionStatus.DRAFT)
    handler, bag = _build(dep)

    await handler.handle(_event())

    bag.validation_service.validate_deposition.assert_not_called()
    bag.record_service.publish_record.assert_not_called()
    bag.feature_service.insert_features_for_record.assert_not_called()
    bag.uow.commit.assert_not_called()


async def test_accepted_before_publish_checkpoint_is_stale_not_resumable() -> None:
    # ACCEPTED admits a delivery only together with the PUBLISHED checkpoint
    # (the crash-during-features case); ACCEPTED with an earlier stage is a
    # stale/foreign state and must short-circuit.
    dep = _make_deposition(status=DepositionStatus.ACCEPTED, stage=SubmissionStage.SUBMITTED)
    handler, bag = _build(dep)

    await handler.handle(_event())

    bag.validation_service.validate_deposition.assert_not_called()
    bag.feature_service.insert_features_for_record.assert_not_called()
    bag.uow.commit.assert_not_called()


async def test_value_error_from_validation_raises_permanent() -> None:
    dep = _make_deposition()
    handler, _bag = _build(dep, validate_error=ValueError("bad config"))

    with pytest.raises(PermanentError) as excinfo:
        await handler.handle(_event())

    assert isinstance(excinfo.value.__cause__, ValueError)


async def test_on_exhausted_pre_publish_returns_to_draft() -> None:
    dep = _make_deposition(stage=SubmissionStage.SUBMITTED)
    handler, bag = _build(dep)

    await handler.on_exhausted(_event())

    assert dep.status == DepositionStatus.DRAFT
    bag.deposition_service.return_to_draft.assert_awaited()
    types = [type(e).__name__ for e in bag.appended]
    assert types == ["ValidationFailed"]
    failed = bag.appended[0]
    assert isinstance(failed, ValidationFailed)
    assert failed.reasons == ["submission processing retries exhausted"]


async def test_on_exhausted_post_publish_no_return_to_draft() -> None:
    dep = _make_deposition(
        status=DepositionStatus.ACCEPTED,
        stage=SubmissionStage.PUBLISHED,
        record_srn=_record_srn(),
    )
    handler, bag = _build(dep)

    await handler.on_exhausted(_event())

    assert dep.status == DepositionStatus.ACCEPTED
    bag.deposition_service.return_to_draft.assert_not_called()
    bag.outbox.append.assert_not_called()


async def test_on_exhausted_missing_deposition_does_not_raise() -> None:
    handler, bag = _build(None)

    await handler.on_exhausted(_event())

    bag.outbox.append.assert_not_called()
    bag.deposition_service.return_to_draft.assert_not_called()


# ── Hardening tests (#160) ────────────────────────────────────────────────────


async def test_zero_hook_submission_publishes_without_features_work() -> None:
    # A convention with no hooks: validation still completes (instant pass) and
    # the full pipeline runs, but every ``expected_features`` list is empty. The
    # orchestrator still *calls* insert_features_for_record — the service no-ops
    # internally on an empty expected set.
    dep = _make_deposition()
    handler, bag = _build(dep)

    await handler.handle(_event(hooks=[]))

    types = [type(e).__name__ for e in bag.appended]
    assert types == ["ValidationCompleted", "DepositionApproved"]
    completed = bag.appended[0]
    assert isinstance(completed, ValidationCompleted)
    assert completed.expected_features == []
    approved = bag.appended[1]
    assert isinstance(approved, DepositionApproved)
    assert approved.expected_features == []

    # publish draft carries an empty expected-features set
    draft = bag.record_service.publish_record.call_args[0][0]
    assert isinstance(draft, RecordDraft)
    assert draft.expected_features == []

    assert dep.stage == SubmissionStage.PUBLISHED
    assert dep.status == DepositionStatus.ACCEPTED

    # the orchestrator still calls the feature stage, with an empty expected set
    bag.feature_service.insert_features_for_record.assert_called_once_with(
        hook_output_dir="/fake/out",
        record_srn=str(bag.rec.srn),
        expected_features=[],
    )


async def test_publish_failure_emits_failed_outcome_and_propagates() -> None:
    # A generic error in the publish stage propagates out of handle(), marks the
    # PUBLISH stage FAILED, and never reaches INSERT_FEATURES. Crucially the
    # VALIDATED checkpoint commit has already landed, so a retry fast-forwards
    # past validation.
    dep = _make_deposition()
    handler, bag = _build(dep)

    async def _boom(_draft: RecordDraft) -> None:
        bag.order.append("publish")
        raise RuntimeError("db down")

    bag.record_service.publish_record.side_effect = _boom

    with pytest.raises(RuntimeError):
        await handler.handle(_event())

    assert (_PS, WorkflowStage.VALIDATE, StageOutcome.RAN) in bag.instr.calls
    assert (_PS, WorkflowStage.CURATE, StageOutcome.RAN) in bag.instr.calls
    assert (_PS, WorkflowStage.PUBLISH, StageOutcome.FAILED) in bag.instr.calls
    # INSERT_FEATURES never opened its span
    assert not any(stage == WorkflowStage.INSERT_FEATURES for _, stage, _ in bag.instr.calls)
    bag.feature_service.insert_features_for_record.assert_not_called()

    # the VALIDATED checkpoint commit landed before publish was attempted
    approved_idx = bag.order.index("append:DepositionApproved")
    publish_idx = bag.order.index("publish")
    assert any(bag.order[i] == "commit" for i in range(approved_idx, publish_idx))


@pytest.mark.parametrize("run_status", [RunStatus.FAILED, RunStatus.REJECTED])
async def test_terminal_verdict_with_no_reasons_still_drafts(run_status: RunStatus) -> None:
    # A terminal verdict whose hook results carry neither an error_message nor a
    # rejection_reason (here: an empty results list) yields an empty reasons list
    # but still returns the deposition to draft and never publishes.
    dep = _make_deposition()
    handler, bag = _build(dep, run_status=run_status, hook_results=[])

    await handler.handle(_event())

    types = [type(e).__name__ for e in bag.appended]
    assert types == ["ValidationFailed"]
    failed = bag.appended[0]
    assert isinstance(failed, ValidationFailed)
    assert failed.reasons == []

    assert dep.status == DepositionStatus.DRAFT
    bag.deposition_service.return_to_draft.assert_awaited()
    bag.record_service.publish_record.assert_not_called()


async def test_redelivery_after_resubmission_revalidates() -> None:
    # Pins that a stale delivery racing a resubmission converges on current
    # deposition state: the deposition is back IN_VALIDATION/SUBMITTED (the user
    # fixed and resubmitted), so re-handling the original event simply
    # re-validates. The cost is a duplicate validation run — accepted by design.
    dep = _make_deposition(status=DepositionStatus.IN_VALIDATION, stage=SubmissionStage.SUBMITTED)
    handler, bag = _build(dep)

    await handler.handle(_event())

    bag.validation_service.validate_deposition.assert_called_once()
    bag.record_service.publish_record.assert_called_once()
    assert dep.stage == SubmissionStage.PUBLISHED
    assert dep.status == DepositionStatus.ACCEPTED


async def test_on_exhausted_noop_when_already_drafted() -> None:
    # Already back in DRAFT (a prior terminal verdict, or a concurrent
    # return-to-draft): on_exhausted must not double-notify — no second
    # return_to_draft and no ValidationFailed.
    dep = _make_deposition(status=DepositionStatus.DRAFT, stage=SubmissionStage.SUBMITTED)
    handler, bag = _build(dep)

    await handler.on_exhausted(_event())

    bag.deposition_service.return_to_draft.assert_not_called()
    bag.outbox.append.assert_not_called()
