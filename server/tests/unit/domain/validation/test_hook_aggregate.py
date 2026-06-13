"""Unit tests for the Hook aggregate (identity + fixed contract + live pointer)."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.shared.error import ReservedNameError
from osa.domain.shared.model.hook import ColumnDef, TableFeatureSpec
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookReleaseId


def _feature() -> TableFeatureSpec:
    return TableFeatureSpec(
        cardinality="many",
        columns=[ColumnDef(name="score", json_type="number", required=True)],
    )


def test_hook_holds_identity_and_no_live_release_initially() -> None:
    hook = Hook(name="pocket_detect", feature=_feature(), created_at=datetime.now(UTC))
    assert hook.name == "pocket_detect"
    assert hook.feature.cardinality == "many"
    assert hook.live_release_id is None


def test_feature_contract_is_frozen() -> None:
    hook = Hook(name="pocket_detect", feature=_feature(), created_at=datetime.now(UTC))
    with pytest.raises(Exception):
        hook.feature = _feature()  # type: ignore[misc]


def test_reserved_name_rejected() -> None:
    with pytest.raises(ReservedNameError):
        Hook(name="records", feature=_feature(), created_at=datetime.now(UTC))


def test_with_live_release_returns_repointed_copy() -> None:
    hook = Hook(name="pocket_detect", feature=_feature(), created_at=datetime.now(UTC))
    rid = HookReleaseId(uuid4())
    advanced = hook.with_live_release(rid)
    assert advanced.live_release_id == rid
    # Original unchanged (immutable aggregate).
    assert hook.live_release_id is None
    # Identity preserved.
    assert advanced.name == hook.name
    assert advanced.feature == hook.feature
