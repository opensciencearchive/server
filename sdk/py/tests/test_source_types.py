"""Tests for SDK Source types (SourceFileRef, SourceRecord)."""

from __future__ import annotations

from datetime import datetime

from pydantic import ValidationError
import pytest


class TestSourceFileRef:
    def test_creates_with_name_and_relative_path(self) -> None:
        from osa.types.source import SourceFileRef

        sf = SourceFileRef(name="structure.cif", relative_path="4HHB/structure.cif")
        assert sf.name == "structure.cif"
        assert sf.relative_path == "4HHB/structure.cif"

    def test_is_frozen(self) -> None:
        from osa.types.source import SourceFileRef

        sf = SourceFileRef(name="structure.cif", relative_path="4HHB/structure.cif")
        with pytest.raises(ValidationError):
            sf.name = "other.cif"  # type: ignore[misc]


class TestSourceRecord:
    def test_creates_with_required_fields(self) -> None:
        from osa.types.source import SourceRecord

        sr = SourceRecord(
            source_id="4HHB",
            metadata={"pdb_id": "4HHB", "title": "Deoxy Human Hemoglobin"},
        )
        assert sr.source_id == "4HHB"
        assert sr.metadata["pdb_id"] == "4HHB"
        assert sr.files == []
        assert sr.fetched_at is None

    def test_creates_with_files(self) -> None:
        from osa.types.source import SourceFileRef, SourceRecord

        sr = SourceRecord(
            source_id="4HHB",
            metadata={"pdb_id": "4HHB"},
            files=[
                SourceFileRef(name="structure.cif", relative_path="4HHB/structure.cif"),
                SourceFileRef(name="structure.pdb", relative_path="4HHB/structure.pdb"),
            ],
        )
        assert len(sr.files) == 2
        assert sr.files[0].name == "structure.cif"

    def test_creates_with_fetched_at(self) -> None:
        from osa.types.source import SourceRecord

        now = datetime(2025, 6, 15, 12, 0, 0)
        sr = SourceRecord(source_id="4HHB", metadata={}, fetched_at=now)
        assert sr.fetched_at == now

    def test_is_frozen(self) -> None:
        from osa.types.source import SourceRecord

        sr = SourceRecord(source_id="4HHB", metadata={})
        with pytest.raises(ValidationError):
            sr.source_id = "other"  # type: ignore[misc]
