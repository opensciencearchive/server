"""Unit tests for HookRunner port interface and HookInputs DTO."""

from pathlib import Path

import pytest

from osa.domain.shared.model.hook import (
    HookDefinition,
)
from osa.domain.validation.model.hook_result import HookResult, HookStatus
from osa.domain.validation.port.hook_runner import HookInputs, HookRunner


class TestHookInputs:
    def test_minimal_construction(self):
        inputs = HookInputs(record_json={"srn": "urn:osa:localhost:rec:123"})
        assert inputs.record_json == {"srn": "urn:osa:localhost:rec:123"}
        assert inputs.files_dir is None
        assert inputs.config is None

    def test_with_files_dir(self):
        files = Path("/tmp/files")
        inputs = HookInputs(record_json={"srn": "test"}, files_dir=files)
        assert inputs.files_dir == files

    def test_with_config(self):
        inputs = HookInputs(
            record_json={"srn": "test"},
            config={"r_min": 3.0, "threshold": 0.5},
        )
        assert inputs.config == {"r_min": 3.0, "threshold": 0.5}

    def test_full_construction(self):
        files = Path("/tmp/data/files")
        inputs = HookInputs(
            record_json={"srn": "urn:osa:localhost:rec:456", "name": "test"},
            files_dir=files,
            config={"key": "value"},
        )
        assert inputs.record_json["name"] == "test"
        assert inputs.files_dir == files
        assert inputs.config == {"key": "value"}

    def test_is_frozen(self):
        inputs = HookInputs(record_json={"srn": "test"})
        with pytest.raises(AttributeError):
            inputs.record_json = {}  # type: ignore[misc]

    def test_is_dataclass(self):
        """HookInputs is a frozen dataclass."""
        import dataclasses

        assert dataclasses.is_dataclass(HookInputs)


class TestHookRunnerProtocol:
    def test_is_runtime_checkable(self):
        """HookRunner uses @runtime_checkable so isinstance checks work."""
        assert hasattr(HookRunner, "__protocol_attrs__") or hasattr(
            HookRunner, "__abstractmethods__"
        )

    def test_concrete_class_satisfies_protocol(self):
        """A class implementing run() satisfies the HookRunner protocol."""

        class FakeRunner:
            async def run(
                self,
                hook: HookDefinition,
                inputs: HookInputs,
                workspace_dir: Path,
            ) -> HookResult:
                return HookResult(
                    hook_name=hook.manifest.name,
                    status=HookStatus.PASSED,
                    features=[],
                    duration_seconds=0.1,
                )

        assert isinstance(FakeRunner(), HookRunner)

    def test_incomplete_class_does_not_satisfy_protocol(self):
        """A class missing run() does not satisfy HookRunner."""

        class NotARunner:
            pass

        assert not isinstance(NotARunner(), HookRunner)

    def test_wrong_signature_still_matches_protocol(self):
        """Protocol only checks method existence at runtime, not exact signature."""

        class LaxRunner:
            async def run(self, *args, **kwargs):
                pass

        # runtime_checkable only checks method names exist, not signatures
        assert isinstance(LaxRunner(), HookRunner)
