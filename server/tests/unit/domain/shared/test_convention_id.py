"""Unit tests for the ConventionSlug value object (a bare slug, #145).

Conventions became unversioned in feature #145: their identity is a bare,
human-readable slug rather than the old ``"<slug>@<version>"`` form. This
suite replaces the former ``ConventionId`` tests.
"""

import pytest

from osa.domain.shared.error import ValidationError
from osa.domain.shared.model.srn import ConventionSlug


def test_construct_and_read_root() -> None:
    slug = ConventionSlug("proteins")
    assert slug.root == "proteins"
    assert str(slug) == "proteins"


def test_parse_round_trips() -> None:
    slug = ConventionSlug.parse("pdb-structure")
    assert slug.root == "pdb-structure"
    assert str(slug) == "pdb-structure"


def test_equality_via_root() -> None:
    assert ConventionSlug("proteins") == ConventionSlug("proteins")
    assert ConventionSlug("proteins").root == "proteins"
    assert ConventionSlug("proteins") != ConventionSlug("genomes")


@pytest.mark.parametrize(
    "value",
    [
        "proteins",
        "pdb-structure",
        "abc",  # minimum length (3)
        "a1b",
        "x" * 64,  # maximum length
    ],
)
def test_valid_slugs_accepted(value: str) -> None:
    assert ConventionSlug(value).root == value


@pytest.mark.parametrize(
    "value",
    [
        "",  # empty
        "ab",  # too short (< 3)
        "x" * 65,  # too long (> 64)
        "1proteins",  # must start with a letter
        "-proteins",  # must start with a letter
        "Proteins",  # uppercase rejected
        "proteins_1",  # underscores rejected
        "proteins@1.0.0",  # version suffix rejected
        "proteins ",  # whitespace rejected (no stripping)
        "urn:osa:localhost:conv:proteins",  # URN form rejected
    ],
)
def test_malformed_rejected(value: str) -> None:
    with pytest.raises(ValueError):
        ConventionSlug(value)


def test_parse_rejects_version_suffix() -> None:
    with pytest.raises(ValueError):
        ConventionSlug.parse("proteins@1.0.0")


def test_is_frozen() -> None:
    slug = ConventionSlug("proteins")
    with pytest.raises(Exception):
        slug.root = "other"  # type: ignore[misc]


def test_is_hashable_and_usable_as_dict_key() -> None:
    slug = ConventionSlug("proteins")
    mapping = {slug: 1}
    assert mapping[ConventionSlug("proteins")] == 1
    assert hash(slug) == hash(ConventionSlug("proteins"))


@pytest.mark.parametrize(
    "title,expected",
    [
        ("PDB Structures", "pdb-structures"),
        ("  Spaces   and   tabs  ", "spaces-and-tabs"),
        ("Punctuation! & symbols?", "punctuation-symbols"),
        ("UPPER_snake_case", "upper-snake-case"),
        ("trailing---hyphens---", "trailing-hyphens"),
        ("Mix3d 123 numbers", "mix3d-123-numbers"),
        ("x" * 100, "x" * 64),  # capped at 64
    ],
)
def test_from_title_slugifies(title: str, expected: str) -> None:
    assert ConventionSlug.from_title(title).root == expected


def test_from_title_is_deterministic() -> None:
    assert ConventionSlug.from_title("PDB Structures") == ConventionSlug.from_title(
        "PDB Structures"
    )


@pytest.mark.parametrize(
    "title",
    [
        "",  # nothing
        "!!!",  # no alphanumerics → empty slug
        "ab",  # slugifies to < 3 chars
        "3D structures",  # slug "3d-structures" starts with a digit
        "42",  # starts with a digit and too short
    ],
)
def test_from_title_rejects_untitleable(title: str) -> None:
    with pytest.raises(ValidationError):
        ConventionSlug.from_title(title)
