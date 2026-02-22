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
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()
        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention()
        file_storage = AsyncMock()

        # ValidationService mock
        validation_service = AsyncMock()
        run_mock = MagicMock()
        run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
        run_mock.status = RunStatus.COMPLETED
        validation_service.create_run.return_value = run_mock
        validation_service.run_repo = AsyncMock()

        handler = ValidateDeposition(
            outbox=outbox,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
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
    async def test_handler_passes_files_dir_from_storage(self):
        """ValidateDeposition must call file_storage.get_files_dir and pass result as files_dir."""
        from pathlib import Path

        outbox = AsyncMock()
        dep = _make_deposition()
        dep.metadata = {"title": "Test"}
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = dep
        convention = _make_convention()
        convention_repo = AsyncMock()
        convention_repo.get.return_value = convention
        file_storage = MagicMock()
        file_storage.get_files_dir.return_value = Path("/data/depositions/test-dep")

        validation_service = AsyncMock()
        run_mock = MagicMock()
        run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
        run_mock.status = RunStatus.COMPLETED
        validation_service.create_run.return_value = run_mock
        validation_service.run_repo = AsyncMock()

        handler = ValidateDeposition(
            outbox=outbox,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
            validation_service=validation_service,
        )

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=dep.srn,
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        file_storage.get_files_dir.assert_called_once_with(dep.srn)
        call_args = validation_service.create_run.call_args
        inputs = call_args[1]["inputs"] if "inputs" in call_args[1] else call_args[0][0]
        assert inputs.files_dir == Path("/data/depositions/test-dep")


class TestValidateDepositionPassesEnvelope:
    @pytest.mark.asyncio
    async def test_handler_passes_envelope_as_record_json(self):
        """ValidateDeposition must wrap metadata in {srn, metadata} envelope."""
        outbox = AsyncMock()
        dep = _make_deposition()
        dep.metadata = {"title": "Test"}
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = dep
        convention = _make_convention()
        convention_repo = AsyncMock()
        convention_repo.get.return_value = convention
        file_storage = AsyncMock()

        validation_service = AsyncMock()
        run_mock = MagicMock()
        run_mock.srn = ValidationRunSRN.parse("urn:osa:localhost:val:run1")
        run_mock.status = RunStatus.COMPLETED
        validation_service.create_run.return_value = run_mock
        validation_service.run_repo = AsyncMock()

        handler = ValidateDeposition(
            outbox=outbox,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
            validation_service=validation_service,
        )

        event = DepositionSubmittedEvent(
            id=EventId(uuid4()),
            deposition_id=dep.srn,
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        # Check the inputs passed to create_run
        call_args = validation_service.create_run.call_args
        inputs = call_args[1]["inputs"] if "inputs" in call_args[1] else call_args[0][0]
        assert inputs.record_json == {"srn": str(dep.srn), "metadata": dep.metadata}


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
    async def test_inserts_features_on_record_published(self):
        """InsertRecordFeatures reads from cold storage and inserts with record_srn."""
        deposition_repo = AsyncMock()
        dep = _make_deposition()
        deposition_repo.get.return_value = dep

        hook = _make_hook_def()
        convention = _make_convention(hooks=[hook])
        convention_repo = AsyncMock()
        convention_repo.get.return_value = convention

        file_storage = AsyncMock()
        file_storage.hook_features_exist.return_value = True
        file_storage.read_hook_features.return_value = [{"score": 0.95}]

        feature_service = AsyncMock()
        feature_service.insert_features.return_value = 1

        handler = InsertRecordFeatures(
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
            feature_service=feature_service,
        )

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=_make_record_srn(),
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        file_storage.hook_features_exist.assert_called_once_with(_make_dep_srn(), "pocket_detect")
        file_storage.read_hook_features.assert_called_once_with(_make_dep_srn(), "pocket_detect")
        feature_service.insert_features.assert_called_once_with(
            hook_name="pocket_detect",
            record_srn=str(_make_record_srn()),
            rows=[{"score": 0.95}],
        )

    @pytest.mark.asyncio
    async def test_skips_when_no_features(self):
        """InsertRecordFeatures skips hooks with no features.json."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        convention = _make_convention(hooks=[_make_hook_def()])
        convention_repo = AsyncMock()
        convention_repo.get.return_value = convention

        file_storage = AsyncMock()
        file_storage.hook_features_exist.return_value = False

        feature_service = AsyncMock()

        handler = InsertRecordFeatures(
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
            feature_service=feature_service,
        )

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=_make_record_srn(),
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        feature_service.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_convention_with_no_hooks(self):
        """InsertRecordFeatures does nothing for conventions without hooks."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        convention = _make_convention(hooks=[])
        convention_repo = AsyncMock()
        convention_repo.get.return_value = convention

        file_storage = AsyncMock()
        feature_service = AsyncMock()

        handler = InsertRecordFeatures(
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
            feature_service=feature_service,
        )

        event = RecordPublished(
            id=EventId(uuid4()),
            record_srn=_make_record_srn(),
            deposition_srn=_make_dep_srn(),
            metadata={"title": "Test"},
        )
        await handler.handle(event)

        file_storage.hook_features_exist.assert_not_called()
        feature_service.insert_features.assert_not_called()
