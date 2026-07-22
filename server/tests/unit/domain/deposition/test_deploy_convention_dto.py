"""The convention wire contract: config/limits are authored (on the component),
`release` is a pure build artifact, and hooks + ingesters are symmetric.

These lock the edge DTOs (`DeployConventionHook`/`DeployConventionRelease`/
`DeployConventionIngester`) and their `to_deploy`/`to_definition` re-gathering
into the *unchanged* internal `OciConfig`/`IngesterDefinition`.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from osa.domain.deposition.command.create_convention import (
    DeployConvention,
    DeployConventionHook,
    DeployConventionIngester,
    DeployConventionRelease,
)


def _hook(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "name": "detect",
        "feature": {
            "cardinality": "many",
            "columns": [{"name": "score", "json_type": "number", "required": True}],
        },
        "config": {"k": "v"},
        "limits": {"memory": "2g"},
        "release": {"image": "reg/x:1", "digest": "sha256:abc", "source_ref": "git:1"},
    }
    base.update(overrides)
    return base


def _ingester(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "name": "ingest",
        "config": {"email": ""},
        "limits": {"memory": "1g"},
        "schedule": {"cron": "0 0 * * *"},
        "release": {"image": "reg/i:1", "digest": "sha256:def", "source_ref": "git:1"},
    }
    base.update(overrides)
    return base


def _body(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "title": "T",
        "description": "d",
        "file_requirements": {
            "accepted_types": [".csv"],
            "max_count": 5,
            "max_file_size": 100,
        },
        "schema": {"id": "s-id", "version": "1.0.0", "fields": []},
        "hooks": [_hook()],
        "ingester": _ingester(),
        "docs": {
            "purpose": "p",
            "example_questions": ["a?", "b?", "c?"],
            "examples": [{"question": "a?", "query": "q", "interpretation": "i"}],
        },
    }
    base.update(overrides)
    return base


# --- release is a pure build artifact ---------------------------------------


def test_release_is_pure_build_artifact() -> None:
    rel = DeployConventionRelease.model_validate(
        {"image": "reg/x:1", "digest": "sha256:abc", "source_ref": "git:1"}
    )
    assert set(rel.model_dump()) == {"image", "digest", "source_ref"}


def test_release_rejects_config_and_limits() -> None:
    # The old shape (config/limits inside release) is a loud 422 now.
    for bad in ("config", "limits"):
        with pytest.raises(ValidationError):
            DeployConventionRelease.model_validate(
                {"image": "i", "digest": "d", "source_ref": "g", bad: {}}
            )


# --- hook: authored config/limits on the component --------------------------


def test_hook_carries_config_and_limits() -> None:
    hook = DeployConventionHook.model_validate(_hook())
    assert hook.config == {"k": "v"}
    assert hook.limits.memory == "2g"
    assert set(hook.release.model_dump()) == {"image", "digest", "source_ref"}


def test_hook_config_is_required() -> None:
    with pytest.raises(ValidationError):
        DeployConventionHook.model_validate(_hook(config=None) | {"config": None})


def test_hook_to_deploy_regathers_into_runtime() -> None:
    hook = DeployConventionHook.model_validate(_hook())
    hd = hook.to_deploy()
    # config/limits come from the hook, image/digest from the release.
    assert hd.runtime.config == {"k": "v"}
    assert hd.runtime.limits.memory == "2g"
    assert hd.runtime.image == "reg/x:1"
    assert hd.runtime.digest == "sha256:abc"
    assert hd.source_ref == "git:1"
    assert hd.identity.name.root == "detect"


# --- ingester: symmetric with hooks -----------------------------------------


def test_ingester_is_symmetric_with_hooks() -> None:
    ing = DeployConventionIngester.model_validate(_ingester())
    # authored config/limits/schedule on the component; build under release.
    assert ing.config == {"email": ""}
    assert ing.limits.memory == "1g"
    assert ing.schedule.cron == "0 0 * * *"
    assert set(ing.release.model_dump()) == {"image", "digest", "source_ref"}


def test_ingester_to_definition_regathers() -> None:
    idef = DeployConventionIngester.model_validate(_ingester()).to_definition()
    assert idef.image == "reg/i:1"
    assert idef.digest == "sha256:def"
    assert idef.config == {"email": ""}
    assert idef.limits.memory == "1g"
    assert idef.source_ref == "git:1"  # provenance carried onto the ingester


def test_ingester_name_accepted_but_not_persisted() -> None:
    # `name` is the cloud build-fan-out key; the internal ingester has none.
    idef = DeployConventionIngester.model_validate(_ingester(name="anything")).to_definition()
    assert "name" not in idef.model_dump()


def test_ingester_rejects_flat_image() -> None:
    # No more flat image/digest — they live under release (extra=forbid).
    with pytest.raises(ValidationError):
        DeployConventionIngester.model_validate(_ingester(image="reg/i:1"))


# --- full body round-trip + optional omission -------------------------------


def test_full_body_validates_and_maps() -> None:
    cmd = DeployConvention.model_validate(_body())
    assert len(cmd.hooks) == 1
    assert cmd.hooks[0].to_deploy().runtime.config == {"k": "v"}
    assert cmd.ingester is not None
    assert cmd.ingester.to_definition().source_ref == "git:1"


def test_optional_fields_may_be_omitted() -> None:
    # exclude_none producer output: omit ingester schedule/initial_run, hook
    # limits (defaulted), docs optionals — the server accepts absence.
    hook = _hook()
    del hook["limits"]
    ing = _ingester()
    del ing["schedule"]
    cmd = DeployConvention.model_validate(_body(hooks=[hook], ingester=ing))
    assert cmd.hooks[0].limits.memory == "1g"  # OciLimits default
    assert cmd.ingester is not None and cmd.ingester.schedule is None


def test_ingester_optional() -> None:
    cmd = DeployConvention.model_validate(_body(ingester=None))
    assert cmd.ingester is None
