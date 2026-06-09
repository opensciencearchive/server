"""T065 — HookDefinition rejects reserved hook names (records, datasets)."""

import pytest

from osa.domain.shared.error import ReservedNameError
from osa.domain.shared.model.hook import (
    ColumnDef,
    HookDefinition,
    OciConfig,
    TableFeatureSpec,
)


def _hook(name: str) -> HookDefinition:
    return HookDefinition(
        name=name,
        runtime=OciConfig(image="ghcr.io/example/hook", digest="sha256:abc123"),
        feature=TableFeatureSpec(
            cardinality="one",
            columns=[ColumnDef(name="score", json_type="number", required=True)],
        ),
    )


@pytest.mark.parametrize("reserved", ["records", "datasets"])
def test_hook_rejects_reserved_name(reserved: str) -> None:
    with pytest.raises(ReservedNameError) as exc:
        _hook(reserved)
    assert exc.value.code == "reserved_name"
    assert exc.value.kind == "hook"
    assert exc.value.name == reserved


def test_hook_allows_non_reserved_name() -> None:
    hook = _hook("chemical_features")
    assert hook.name == "chemical_features"
