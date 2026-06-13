"""Unit tests for HookRelease (immutable versioned artifact)."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.shared.model.hook import OciConfig, OciLimits
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId


def _release(version: int = 1, memory: str = "1g") -> HookRelease:
    return HookRelease(
        id=HookReleaseId(uuid4()),
        hook_name="pocket_detect",
        version=version,
        runtime=OciConfig(image="reg/p:abc", digest="sha256:abc", limits=OciLimits(memory=memory)),
        source_ref="git-abc123",
        built_by="deploy-bot",
        built_at=datetime.now(UTC),
    )


def test_release_carries_runtime_and_source_ref() -> None:
    rel = _release()
    assert rel.runtime.image == "reg/p:abc"
    assert rel.runtime.digest == "sha256:abc"
    assert rel.source_ref == "git-abc123"
    assert rel.version == 1


def test_release_is_immutable() -> None:
    rel = _release()
    with pytest.raises(Exception):
        rel.version = 2  # type: ignore[misc]


def test_source_ref_required() -> None:
    with pytest.raises(Exception):
        HookRelease(
            id=HookReleaseId(uuid4()),
            hook_name="pocket_detect",
            version=1,
            runtime=OciConfig(image="i", digest="sha256:d"),
            built_at=datetime.now(UTC),
        )  # type: ignore[call-arg]


def test_with_doubled_memory_for_oom_retry() -> None:
    rel = _release(memory="1g")
    doubled = rel.with_doubled_memory()
    assert doubled.runtime.limits.memory == "2g"
    # Doubling is an in-memory view for retries; identity/version preserved.
    assert doubled.version == rel.version
    assert doubled.id == rel.id
    # Original untouched.
    assert rel.runtime.limits.memory == "1g"
