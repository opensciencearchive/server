"""T028 — RESERVED_NAMES constant and ReservedNameError."""

from osa.domain.shared.error import DomainError, ReservedNameError
from osa.domain.shared.model.reserved import RESERVED_NAMES


def test_reserved_names_contains_records_and_datasets() -> None:
    assert RESERVED_NAMES == frozenset({"records", "datasets"})


def test_reserved_names_is_frozen() -> None:
    assert isinstance(RESERVED_NAMES, frozenset)


def test_reserved_name_error_is_domain_error() -> None:
    err = ReservedNameError("records", "schema")
    assert isinstance(err, DomainError)


def test_reserved_name_error_code_and_message() -> None:
    err = ReservedNameError("records", "schema")
    assert err.code == "reserved_name"
    assert err.name == "records"
    assert err.kind == "schema"
    assert "records" in err.message
    assert "/data/" in err.message


def test_reserved_name_error_lists_reserved_names() -> None:
    err = ReservedNameError("datasets", "hook")
    # sorted reserved set is rendered in the message
    assert "datasets" in err.message
    assert "records" in err.message
