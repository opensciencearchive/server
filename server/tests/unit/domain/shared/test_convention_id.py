"""Unit tests for the ConventionId value object (mirrors SchemaId)."""

import pytest

from osa.domain.shared.model.srn import ConventionId, LocalId, Semver


def test_parse_round_trips_render() -> None:
    cid = ConventionId.parse("proteins@1.0.0")
    assert cid.id == LocalId("proteins")
    assert cid.version == Semver.from_string("1.0.0")
    assert cid.render() == "proteins@1.0.0"
    assert str(cid) == "proteins@1.0.0"


def test_major_is_first_semver_component() -> None:
    assert ConventionId.parse("proteins@2.5.1").major == 2
    assert ConventionId.parse("proteins@0.9.0").major == 0


def test_construct_directly() -> None:
    cid = ConventionId(id=LocalId("proteins"), version=Semver.from_string("3.0.0"))
    assert cid.render() == "proteins@3.0.0"


@pytest.mark.parametrize(
    "value",
    [
        "proteins",  # no version
        "proteins@",  # empty version
        "proteins@notsemver",  # bad version
        "@1.0.0",  # empty id
        "",  # empty
    ],
)
def test_malformed_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        ConventionId.parse(value)


def test_is_frozen_value_object() -> None:
    cid = ConventionId.parse("proteins@1.0.0")
    with pytest.raises(Exception):
        cid.id = LocalId("other")  # type: ignore[misc]
