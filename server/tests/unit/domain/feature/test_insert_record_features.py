"""Unit tests for InsertRecordFeatures event handler and FeatureService.insert_features_for_record."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.convention import Convention
from osa.domain.deposition.model.value import DepositionStatus, FileRequirements
from osa.domain.feature.handler.insert_record_features import InsertRecordFeatures
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
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
)


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_conv_srn() -> ConventionSRN:
    return ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")


def _make_record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


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


def _make_event() -> RecordPublished:
    return RecordPublished(
        id=EventId(uuid4()),
        record_srn=_make_record_srn(),
        deposition_srn=_make_dep_srn(),
        metadata={"title": "Test"},
    )


def _make_feature_service(
    feature_store: AsyncMock | None = None,
    deposition_repo: AsyncMock | None = None,
    convention_repo: AsyncMock | None = None,
    file_storage: AsyncMock | None = None,
) -> FeatureService:
    return FeatureService(
        feature_store=feature_store or AsyncMock(),
        deposition_repo=deposition_repo or AsyncMock(),
        convention_repo=convention_repo or AsyncMock(),
        file_storage=file_storage or AsyncMock(),
    )


def _make_handler(
    feature_service: FeatureService | AsyncMock | None = None,
) -> InsertRecordFeatures:
    return InsertRecordFeatures(
        feature_service=feature_service or AsyncMock(),
    )


class TestInsertRecordFeaturesHandler:
    @pytest.mark.asyncio
    async def test_delegates_to_feature_service(self):
        """Handler delegates to FeatureService.insert_features_for_record."""
        feature_service = AsyncMock()
        handler = _make_handler(feature_service=feature_service)

        event = _make_event()
        await handler.handle(event)

        feature_service.insert_features_for_record.assert_called_once_with(
            event.deposition_srn, str(event.record_srn)
        )


class TestFeatureServiceInsertFeaturesForRecord:
    @pytest.mark.asyncio
    async def test_inserts_features_from_cold_storage(self):
        """Reads features.json from cold storage and inserts with record_srn."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        hook = _make_hook_def()
        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention(hooks=[hook])

        file_storage = AsyncMock()
        file_storage.hook_features_exist.return_value = True
        file_storage.read_hook_features.return_value = [{"score": 0.95}, {"score": 0.82}]

        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 2

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        feature_store.insert_features.assert_called_once_with(
            "pocket_detect",
            str(_make_record_srn()),
            [{"score": 0.95}, {"score": 0.82}],
        )

    @pytest.mark.asyncio
    async def test_skips_hooks_without_features_file(self):
        """Hooks that didn't produce features.json are skipped."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention(hooks=[_make_hook_def()])

        file_storage = AsyncMock()
        file_storage.hook_features_exist.return_value = False

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        file_storage.read_hook_features.assert_not_called()
        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_empty_feature_list(self):
        """Hooks that produced empty features.json are skipped."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention(hooks=[_make_hook_def()])

        file_storage = AsyncMock()
        file_storage.hook_features_exist.return_value = True
        file_storage.read_hook_features.return_value = []

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_multiple_hooks(self):
        """Processes all hooks in the convention."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        hooks = [_make_hook_def("hook_a"), _make_hook_def("hook_b")]
        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention(hooks=hooks)

        file_storage = AsyncMock()
        file_storage.hook_features_exist.return_value = True
        file_storage.read_hook_features.side_effect = [
            [{"score": 0.9}],
            [{"score": 0.8}],
        ]

        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 1

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        assert feature_store.insert_features.call_count == 2

    @pytest.mark.asyncio
    async def test_convention_with_no_hooks(self):
        """No-op for conventions without hooks."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        convention_repo = AsyncMock()
        convention_repo.get.return_value = _make_convention(hooks=[])

        file_storage = AsyncMock()
        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
            file_storage=file_storage,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        file_storage.hook_features_exist.assert_not_called()
        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_deposition_logs_and_returns(self):
        """Missing deposition is a no-op (logged error)."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = None

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_convention_logs_and_returns(self):
        """Missing convention is a no-op (logged error)."""
        deposition_repo = AsyncMock()
        deposition_repo.get.return_value = _make_deposition()

        convention_repo = AsyncMock()
        convention_repo.get.return_value = None

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            deposition_repo=deposition_repo,
            convention_repo=convention_repo,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()))

        feature_store.insert_features.assert_not_called()
