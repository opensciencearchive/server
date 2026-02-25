"""Unit tests verifying the event chain: DepositionSubmitted → Validate → Approve → Record."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.curation.handler.auto_approve_curation import AutoApproveCuration
from osa.domain.deposition.event.submitted import DepositionSubmittedEvent
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import DepositionStatus, FileRequirements
from osa.domain.feature.handler.insert_record_features import InsertRecordFeatures
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.record.handler.convert_deposition_to_record import ConvertDepositionToRecord
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import (
    ColumnDef,
    FeatureSchema,
    HookDefinition,
    HookManifest,
)
from osa.domain.shared.model.srn import (
    ConventionSRN,
    DepositionSRN,
    RecordSRN,
    SchemaSRN,
    ValidationRunSRN,
)
from osa.domain.validation.event.validation_completed import ValidationCompleted
from osa.domain.validation.handler.validate_deposition import ValidateDeposition
from osa.domain.validation.model import RunStatus


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


def _make_deposition() -> Deposition:
    return Deposition(
        srn=_make_dep_srn(),
        convention_srn=_make_conv_srn(),
        status=DepositionStatus.IN_VALIDATION,
        owner_id=UserId(uuid4()),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


def _make_convention(hooks: list[HookDefinition] | None = None) -> Convention:
    return Convention(
        srn=_make_conv_srn(),
        title="Test",
        schema_srn=SchemaSRN.parse("urn:osa:localhost:schema:test@1.0.0"),
        file_requirements=FileRequirements(
            accepted_types=[".csv"],
            min_count=1,
            max_count=3,
            max_file_size=1_000_000,
        ),
        hooks=hooks or [],
        created_at=datetime.now(UTC),
    )


def _make_hook_def(name: str = "pocket_detect") -> HookDefinition:
    return HookDefinition(
        image="ghcr.io/example/hook",
        digest="sha256:abc123",
        manifest=HookManifest(
            name=name,
            record_schema="SampleSchema",
            cardinality="one",
            feature_schema=FeatureSchema(
                columns=[ColumnDef(name="score", json_type="number", required=True)],
            ),
        ),
    )


class TestValidateDepositionEmitsCompleted:
    @pytest.mark.asyncio
    async def test_emits_validation_completed(self):
        outbox = AsyncMock()
        dep = _make_deposition()

        # ValidationService mock — validate_deposition returns (run, results, dep)
        validation_service = AsyncMock()
        run_mock = MagicMock()
        run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
        run_mock.status = RunStatus.COMPLETED
        validation_service.validate_deposition.return_value = (run_mock, [], dep)

        handler = ValidateDeposition(
            outbox=outbox,
            validation_service=validation_service,
        )

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ValidationCompleted)
        assert emitted.deposition_srn == _make_dep_srn()
        assert emitted.status == RunStatus.COMPLETED


class TestValidateDepositionPassesFilesDir:
    @pytest.mark.asyncio
    async def test_handler_delegates_to_service(self):
        """ValidateDeposition delegates to validation_service.validate_deposition."""
        outbox = AsyncMock()
        dep = _make_deposition()

        validation_service = AsyncMock()
        run_mock = MagicMock()
        run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
        run_mock.status = RunStatus.COMPLETED
        validation_service.validate_deposition.return_value = (run_mock, [], dep)

        handler = ValidateDeposition(
            outbox=outbox,
            validation_service=validation_service,
        )

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=dep.srn,
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        validation_service.validate_deposition.assert_called_once_with(dep.srn)


class TestValidateDepositionPassesEnvelope:
    @pytest.mark.asyncio
    async def test_handler_emits_failed_on_rejected(self):
        """ValidateDeposition emits ValidationFailed when run status is REJECTED."""
        from osa.domain.validation.event.validation_failed import ValidationFailed
        from osa.domain.validation.model.hook_result import HookResult, HookStatus

        outbox = AsyncMock()
        dep = _make_deposition()

        validation_service = AsyncMock()
        run_mock = MagicMock()
        run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
        run_mock.status = RunStatus.REJECTED
        hook_result = HookResult(
            hook_name="pocket_detect",
            status=HookStatus.REJECTED,
            rejection_reason="Data quality too low",
            duration_seconds=1.0,
        )
        validation_service.validate_deposition.return_value = (
            run_mock,
            [hook_result],
            dep,
        )

        handler = ValidateDeposition(
            outbox=outbox,
            validation_service=validation_service,
        )

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=dep.srn,
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_called_once()
        emitted = outbox.append.call_args[0][0]
        assert isinstance(emitted, ValidationFailed)
        assert emitted.status == RunStatus.REJECTED


class TestAutoApproveCurationEmitsApproved:
    @pytest.mark.asyncio
    async def test_auto_approve_on_completed(self):
        outbox = AsyncMock()
        handler = AutoApproveCuration(outbox=outbox)

        event = ValidationCompleted(
            id=EventId(uuid4()),
            validation_run_srn=ValidationRunSRN.parse("urn:osa:localhost:val:run1"),
            deposition_srn=_make_dep_srn(),
            convention_srn=_make_conv_srn(),
            status=RunStatus.COMPLETED,
            hook_results=[],
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_called_once()

    @pytest.mark.asyncio
    async def test_skips_approve_on_failed(self):
        outbox = AsyncMock()
        handler = AutoApproveCuration(outbox=outbox)

        event = ValidationCompleted(
            id=EventId(uuid4()),
            validation_run_srn=ValidationRunSRN.parse("urn:osa:localhost:val:run1"),
            deposition_srn=_make_dep_srn(),
            convention_srn=_make_conv_srn(),
            status=RunStatus.FAILED,
            hook_results=[],
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        outbox.append.assert_not_called()


class TestConvertDepositionToRecord:
    @pytest.mark.asyncio
    async def test_publishes_record(self):
        service = AsyncMock()
        handler = ConvertDepositionToRecord(service=service)

        from osa.domain.curation.event.deposition_approved import DepositionApproved

        event = DepositionApproved(
            id=EventId(uuid4()),
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        service.publish_record.assert_called_once_with(
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )


class TestInsertRecordFeatures:
    @pytest.mark.asyncio
    async def test_delegates_to_feature_service(self):
        """InsertRecordFeatures delegates to feature_service.insert_features_for_record."""
        feature_service = AsyncMock()

        handler = InsertRecordFeatures(
            feature_service=feature_service,
        )

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=_make_record_srn(),
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        feature_service.insert_features_for_record.assert_called_once_with(
            _make_dep_srn(), str(_make_record_srn())
        )
