"""Tests for validation domain models: HookResult, HookStatus, ProgressEntry."""


def test_hook_status_values():
    from osa.domain.validation.model.hook_result import HookStatus

    assert HookStatus.PASSED == "passed"
    assert HookStatus.REJECTED == "rejected"
    assert HookStatus.FAILED == "failed"


def test_progress_entry_full():
    from osa.domain.validation.model.hook_result import ProgressEntry

    entry = ProgressEntry(step="Loading structure", status="running", message="Parsing atoms...")
    assert entry.step == "Loading structure"
    assert entry.status == "running"
    assert entry.message == "Parsing atoms..."


def test_progress_entry_minimal():
    from osa.domain.validation.model.hook_result import ProgressEntry

    entry = ProgressEntry(status="completed")
    assert entry.step is None
    assert entry.message is None


def test_progress_entry_rejection():
    from osa.domain.validation.model.hook_result import ProgressEntry

    entry = ProgressEntry(status="rejected", message="Bad data")
    assert entry.status == "rejected"


def test_hook_result_passed():
    from osa.domain.validation.model.hook_result import HookResult, HookStatus

    result = HookResult(
        hook_name="detect_pockets",
        status=HookStatus.PASSED,
        features=[{"pocket_id": "P1", "score": 0.85}],
        duration_seconds=12.5,
    )
    assert result.hook_name == "detect_pockets"
    assert result.status == HookStatus.PASSED
    assert len(result.features) == 1
    assert result.rejection_reason is None
    assert result.error_message is None
    assert result.duration_seconds == 12.5


def test_hook_result_rejected():
    from osa.domain.validation.model.hook_result import HookResult, HookStatus

    result = HookResult(
        hook_name="check_structure",
        status=HookStatus.REJECTED,
        features=[],
        rejection_reason="Missing coordinates",
        duration_seconds=2.1,
    )
    assert result.status == HookStatus.REJECTED
    assert result.rejection_reason == "Missing coordinates"
    assert result.features == []


def test_hook_result_failed():
    from osa.domain.validation.model.hook_result import HookResult, HookStatus

    result = HookResult(
        hook_name="detect_pockets",
        status=HookStatus.FAILED,
        features=[],
        error_message="OOM killed",
        duration_seconds=300.0,
    )
    assert result.status == HookStatus.FAILED
    assert result.error_message == "OOM killed"


def test_hook_result_with_progress():
    from osa.domain.validation.model.hook_result import (
        HookResult,
        HookStatus,
        ProgressEntry,
    )

    result = HookResult(
        hook_name="detect_pockets",
        status=HookStatus.PASSED,
        features=[{"pocket_id": "P1", "score": 0.9}],
        progress=[
            ProgressEntry(step="Loading", status="completed", message="Done"),
            ProgressEntry(step="Detecting", status="completed", message="Found 1"),
        ],
        duration_seconds=5.0,
    )
    assert len(result.progress) == 2
    assert result.progress[0].step == "Loading"


def test_hook_result_default_progress_empty():
    from osa.domain.validation.model.hook_result import HookResult, HookStatus

    result = HookResult(
        hook_name="x",
        status=HookStatus.PASSED,
        features=[],
        duration_seconds=0.1,
    )
    assert result.progress == []


def test_hook_result_serialization_roundtrip():
    from osa.domain.validation.model.hook_result import (
        HookResult,
        HookStatus,
        ProgressEntry,
    )

    result = HookResult(
        hook_name="detect_pockets",
        status=HookStatus.PASSED,
        features=[{"a": 1}, {"a": 2}],
        progress=[ProgressEntry(step="step1", status="completed")],
        duration_seconds=7.3,
    )
    data = result.model_dump()
    restored = HookResult.model_validate(data)
    assert restored == result
