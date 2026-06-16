"""Unit tests for centralized HTTP mapping of hook-registry domain errors (#145, T061).

The hook deploy / release / live / catalog endpoints raise the shared domain
errors; these assert ``map_osa_error`` turns each into the contract's status
code (see specs/145-feat-hook-versioning/contracts/hook-releases.md).
"""

from __future__ import annotations

from osa.application.api.v1.errors import map_osa_error
from osa.domain.shared.error import (
    ConflictError,
    NotFoundError,
    ReservedNameError,
    ValidationError,
)


def test_feature_contract_mismatch_is_409() -> None:
    # Hook exists with a different fixed feature contract (FR-002/FR-016).
    exc = map_osa_error(ConflictError("different feature contract"))
    assert exc.status_code == 409


def test_unknown_hook_or_release_is_404() -> None:
    exc = map_osa_error(NotFoundError("Hook not found: pocket_detect"))
    assert exc.status_code == 404


def test_malformed_release_is_422() -> None:
    exc = map_osa_error(ValidationError("missing source_ref", field="source_ref"))
    assert exc.status_code == 422
    assert exc.detail["field"] == "source_ref"


def test_reserved_hook_name_is_400() -> None:
    exc = map_osa_error(ReservedNameError("records", "hook"))
    assert exc.status_code == 400


def test_detail_carries_code_and_message() -> None:
    exc = map_osa_error(ConflictError("clash", code="hook_contract_mismatch"))
    assert exc.detail["code"] == "hook_contract_mismatch"
    assert exc.detail["message"] == "clash"
