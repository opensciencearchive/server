"""Unit tests for Deposition aggregate."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from osa.domain.auth.model.value import UserId
from osa.domain.deposition.model.aggregate import Deposition
from osa.domain.deposition.model.value import DepositionFile, DepositionStatus
from osa.domain.shared.error import InvalidStateError
from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN


def _make_dep_srn(id: str = "test-dep") -> DepositionSRN:
    return DepositionSRN.parse(f"urn:osa:localhost:dep:{id}")


def _make_conv_srn(id: str = "test-conv", version: str = "1.0.0") -> ConventionSRN:
    return ConventionSRN.parse(f"urn:osa:localhost:conv:{id}@{version}")


def _make_deposition(**overrides) -> Deposition:
    defaults = dict(
        srn=_make_dep_srn(),
        convention_srn=_make_conv_srn(),
        owner_id=UserId(uuid4()),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )
    defaults.update(overrides)
    return Deposition(**defaults)


def _make_file(name: str = "data.csv", size: int = 1000) -> DepositionFile:
    return DepositionFile(
        name=name,
        size=size,
        checksum="abc123",
        uploaded_at=datetime.now(UTC),
    )


class TestDepositionCreation:
    def test_create_with_convention_srn(self):
        dep = _make_deposition()
        assert dep.convention_srn == _make_conv_srn()
        assert dep.status == DepositionStatus.DRAFT

    def test_create_with_empty_metadata(self):
        dep = _make_deposition()
        assert dep.metadata == {}

    def test_create_with_empty_files(self):
        dep = _make_deposition()
        assert dep.files == []

    def test_create_has_owner(self):
        owner = UserId(uuid4())
        dep = _make_deposition(owner_id=owner)
        assert dep.owner_id == owner


class TestDepositionMetadata:
    def test_update_metadata_in_draft(self):
        dep = _make_deposition()
        dep.update_metadata({"title": "Test"})
        assert dep.metadata == {"title": "Test"}

    def test_update_metadata_rejects_non_draft(self):
        dep = _make_deposition(status=DepositionStatus.IN_VALIDATION)
        with pytest.raises(InvalidStateError):
            dep.update_metadata({"title": "Test"})


class TestDepositionFiles:
    def test_add_file_in_draft(self):
        dep = _make_deposition()
        f = _make_file()
        dep.add_file(f)
        assert len(dep.files) == 1
        assert dep.files[0].name == "data.csv"

    def test_remove_file_in_draft(self):
        dep = _make_deposition()
        dep.add_file(_make_file("a.csv"))
        dep.add_file(_make_file("b.csv"))
        removed = dep.remove_file("a.csv")
        assert removed.name == "a.csv"
        assert len(dep.files) == 1

    def test_remove_nonexistent_file_raises(self):
        dep = _make_deposition()
        from osa.domain.shared.error import NotFoundError

        with pytest.raises(NotFoundError):
            dep.remove_file("no-such-file.csv")

    def test_add_file_rejects_non_draft(self):
        dep = _make_deposition(status=DepositionStatus.IN_VALIDATION)
        with pytest.raises(InvalidStateError):
            dep.add_file(_make_file())

    def test_remove_file_rejects_non_draft(self):
        dep = _make_deposition()
        dep.add_file(_make_file())
        dep.submit()
        with pytest.raises(InvalidStateError):
            dep.remove_file("data.csv")


class TestDepositionSubmit:
    def test_submit_transitions_to_in_validation(self):
        dep = _make_deposition()
        dep.submit()
        assert dep.status == DepositionStatus.IN_VALIDATION

    def test_submit_rejects_non_draft(self):
        dep = _make_deposition(status=DepositionStatus.IN_VALIDATION)
        with pytest.raises(InvalidStateError):
            dep.submit()

    def test_return_to_draft_from_in_validation(self):
        dep = _make_deposition()
        dep.submit()
        dep.return_to_draft()
        assert dep.status == DepositionStatus.DRAFT

    def test_return_to_draft_rejects_non_in_validation(self):
        dep = _make_deposition()
        with pytest.raises(InvalidStateError):
            dep.return_to_draft()
