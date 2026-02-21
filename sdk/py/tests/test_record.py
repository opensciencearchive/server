"""Tests for Record[T]."""

from datetime import datetime
from pathlib import Path
from typing import Literal

from osa import Field, MetadataSchema, Record
from osa.types.files import FileCollection

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "files"


class ProteinStructure(MetadataSchema):
    """Test schema."""

    organism: str
    method: Literal["xray", "cryo-em", "nmr", "predicted"]
    resolution: float | None = Field(default=None, ge=0, le=100, unit="Å")
    uniprot_id: str = Field(pattern=r"^[A-Z0-9]{6,10}$")


class TestRecord:
    def test_metadata_access(self) -> None:
        meta = ProteinStructure(
            organism="H. sapiens", method="xray", resolution=2.1, uniprot_id="P12345"
        )
        files = FileCollection(FIXTURES_DIR)
        rec = Record(
            id="rec-001",
            created_at=datetime(2025, 1, 1),
            metadata=meta,
            files=files,
        )
        assert rec.metadata.organism == "H. sapiens"
        assert rec.metadata.resolution == 2.1

    def test_id_returns_string(self) -> None:
        meta = ProteinStructure(
            organism="H. sapiens", method="xray", uniprot_id="P12345"
        )
        rec = Record(
            id="rec-002",
            created_at=datetime(2025, 1, 1),
            metadata=meta,
            files=FileCollection(FIXTURES_DIR),
        )
        assert rec.id == "rec-002"

    def test_created_at_returns_datetime(self) -> None:
        meta = ProteinStructure(
            organism="H. sapiens", method="xray", uniprot_id="P12345"
        )
        ts = datetime(2025, 6, 15, 12, 0, 0)
        rec = Record(
            id="rec-003",
            created_at=ts,
            metadata=meta,
            files=FileCollection(FIXTURES_DIR),
        )
        assert rec.created_at == ts

    def test_files_returns_file_collection(self) -> None:
        meta = ProteinStructure(
            organism="H. sapiens", method="xray", uniprot_id="P12345"
        )
        files = FileCollection(FIXTURES_DIR)
        rec = Record(
            id="rec-004", created_at=datetime(2025, 1, 1), metadata=meta, files=files
        )
        assert isinstance(rec.files, FileCollection)
        assert len(rec.files) >= 2

    def test_empty_file_collection(self, tmp_path: Path) -> None:
        meta = ProteinStructure(
            organism="H. sapiens", method="xray", uniprot_id="P12345"
        )
        files = FileCollection(tmp_path)
        rec = Record(
            id="rec-005", created_at=datetime(2025, 1, 1), metadata=meta, files=files
        )
        assert len(rec.files) == 0
