"""Unit tests for the HookIdentity→HookIdentity split.

The execution/runtime half (runtime + source_ref) now lives on the
``HookRelease`` entity in the ``validation`` domain (see
``tests/unit/validation/test_hook_release.py``); this identity holds only the
name + the fixed output contract.
"""

import pytest

from osa.domain.shared.error import ReservedNameError
from osa.domain.shared.model.hook import ColumnDef, HookIdentity, TableFeatureSpec


def _feature() -> TableFeatureSpec:
    return TableFeatureSpec(
        cardinality="many",
        columns=[ColumnDef(name="score", json_type="number", required=True)],
    )


def test_identity_holds_name_and_feature_only() -> None:
    ident = HookIdentity(name="pocket_detect", feature=_feature())
    assert ident.name.root == "pocket_detect"
    assert ident.feature.cardinality == "many"
    # Runtime no longer lives on the identity — it moved to HookRelease.
    assert not hasattr(ident, "runtime")


def test_identity_rejects_reserved_name() -> None:
    with pytest.raises(ReservedNameError):
        HookIdentity(name="records", feature=_feature())


def test_identity_is_frozen() -> None:
    ident = HookIdentity(name="pocket_detect", feature=_feature())
    with pytest.raises(Exception):
        ident.name = "other"  # type: ignore[misc]
