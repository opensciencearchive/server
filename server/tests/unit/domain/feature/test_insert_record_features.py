"""Unit tests for InsertRecordFeatures event handler and FeatureService.insert_features_for_record."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.feature.handler.insert_record_features import InsertRecordFeatures
from osa.domain.feature.service.feature import FeatureService
from osa.domain.record.event.record_published import RecordPublished
from osa.domain.shared.event import EventId
from osa.domain.shared.model.hook import ColumnDef
from osa.domain.shared.model.hook_snapshot import HookSnapshot
from osa.domain.shared.model.srn import (
    DepositionSRN,
    RecordSRN,
)


def _make_dep_srn() -> DepositionSRN:
    return DepositionSRN.parse("urn:osa:localhost:dep:test-dep")


def _make_record_srn() -> RecordSRN:
    return RecordSRN.parse("urn:osa:localhost:rec:test-rec@1")


def _make_hook_snapshot(name: str = "pocket_detect") -> HookSnapshot:
    return HookSnapshot(
        name=name,
        image="ghcr.io/example/hook",
        digest="sha256:abc123",
        features=[ColumnDef(name="score", json_type="number", required=True)],
        config={},
    )


def _make_event(hooks: list[HookSnapshot] | None = None) -> RecordPublished:
    return RecordPublished(
        id=EventId(uuid4()),
        record_srn=_make_record_srn(),
        deposition_srn=_make_dep_srn(),
        metadata={"title": "Test"},
        hooks=hooks or [],
    )


def _make_feature_service(
    feature_store: AsyncMock | None = None,
    feature_storage: AsyncMock | None = None,
) -> FeatureService:
    return FeatureService(
        feature_store=feature_store or AsyncMock(),
        feature_storage=feature_storage or AsyncMock(),
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

        hooks = [_make_hook_snapshot()]
        event = _make_event(hooks=hooks)
        await handler.handle(event)

        feature_service.insert_features_for_record.assert_called_once_with(
            deposition_srn=event.deposition_srn,
            record_srn=str(event.record_srn),
            hooks=event.hooks,
        )


class TestFeatureServiceInsertFeaturesForRecord:
    @pytest.mark.asyncio
    async def test_inserts_features_from_cold_storage(self):
        """Reads features.json from cold storage and inserts with record_srn."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.return_value = [{"score": 0.95}, {"score": 0.82}]

        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 2

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        hooks = [_make_hook_snapshot()]
        await service.insert_features_for_record(
            _make_dep_srn(), str(_make_record_srn()), hooks=hooks
        )

        feature_store.insert_features.assert_called_once_with(
            "pocket_detect",
            str(_make_record_srn()),
            [{"score": 0.95}, {"score": 0.82}],
        )

    @pytest.mark.asyncio
    async def test_skips_hooks_without_features_file(self):
        """Hooks that didn't produce features.json are skipped."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = False

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        hooks = [_make_hook_snapshot()]
        await service.insert_features_for_record(
            _make_dep_srn(), str(_make_record_srn()), hooks=hooks
        )

        feature_storage.read_hook_features.assert_not_called()
        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_skips_empty_feature_list(self):
        """Hooks that produced empty features.json are skipped."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.return_value = []

        feature_store = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        hooks = [_make_hook_snapshot()]
        await service.insert_features_for_record(
            _make_dep_srn(), str(_make_record_srn()), hooks=hooks
        )

        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_handles_multiple_hooks(self):
        """Processes all hooks in the event payload."""
        feature_storage = AsyncMock()
        feature_storage.hook_features_exist.return_value = True
        feature_storage.read_hook_features.side_effect = [
            [{"score": 0.9}],
            [{"score": 0.8}],
        ]

        feature_store = AsyncMock()
        feature_store.insert_features.return_value = 1

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        hooks = [_make_hook_snapshot("hook_a"), _make_hook_snapshot("hook_b")]
        await service.insert_features_for_record(
            _make_dep_srn(), str(_make_record_srn()), hooks=hooks
        )

        assert feature_store.insert_features.call_count == 2

    @pytest.mark.asyncio
    async def test_no_hooks_is_noop(self):
        """No-op when hooks list is empty."""
        feature_store = AsyncMock()
        feature_storage = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(_make_dep_srn(), str(_make_record_srn()), hooks=[])

        feature_storage.hook_features_exist.assert_not_called()
        feature_store.insert_features.assert_not_called()

    @pytest.mark.asyncio
    async def test_none_hooks_is_noop(self):
        """No-op when hooks is None."""
        feature_store = AsyncMock()
        feature_storage = AsyncMock()

        service = _make_feature_service(
            feature_store=feature_store,
            feature_storage=feature_storage,
        )

        await service.insert_features_for_record(
            _make_dep_srn(), str(_make_record_srn()), hooks=None
        )

        feature_storage.hook_features_exist.assert_not_called()
        feature_store.insert_features.assert_not_called()
