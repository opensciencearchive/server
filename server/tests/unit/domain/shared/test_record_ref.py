"""RecordRef — parse of ``{id}`` / ``{id}@{version}`` URL segments."""

import pytest

from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.ids import RecordRef


def test_parse_bare_id_has_no_version() -> None:
    ref = RecordRef.parse("0190a1b2")
    assert ref.id == "0190a1b2"
    assert ref.version is None


def test_parse_id_with_version() -> None:
    ref = RecordRef.parse("0190a1b2@3")
    assert ref.id == "0190a1b2"
    assert ref.version == 3


def test_parse_non_integer_version_raises_validation_error() -> None:
    with pytest.raises(ValidationError):
        RecordRef.parse("0190a1b2@latest")


def test_render_round_trips() -> None:
    assert RecordRef.parse("abc@7").render() == "abc@7"
    assert RecordRef.parse("abc").render() == "abc"
