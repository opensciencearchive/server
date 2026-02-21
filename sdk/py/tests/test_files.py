"""Tests for File and FileCollection."""

from pathlib import Path

import pytest

from osa.types.files import File, FileCollection

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "files"


class TestFile:
    def test_name_returns_filename(self) -> None:
        f = File(FIXTURES_DIR / "sample.pdb")
        assert f.name == "sample.pdb"

    def test_size_returns_bytes(self) -> None:
        f = File(FIXTURES_DIR / "sample.pdb")
        assert f.size > 0

    def test_read_returns_bytes(self) -> None:
        f = File(FIXTURES_DIR / "sample.pdb")
        content = f.read()
        assert isinstance(content, bytes)
        assert b"ATOM" in content


class TestFileCollection:
    def test_list_returns_all_files(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        files = col.list()
        assert len(files) >= 2
        names = {f.name for f in files}
        assert "sample.pdb" in names
        assert "ligands.sdf" in names

    def test_glob_matches(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        pdb_files = col.glob("*.pdb")
        assert len(pdb_files) == 1
        assert pdb_files[0].name == "sample.pdb"

    def test_glob_no_match(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        assert col.glob("*.xyz") == []

    def test_getitem_found(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        f = col["sample.pdb"]
        assert f.name == "sample.pdb"

    def test_getitem_not_found(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        with pytest.raises(KeyError):
            col["nonexistent.txt"]

    def test_iter(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        names = [f.name for f in col]
        assert "sample.pdb" in names

    def test_len(self) -> None:
        col = FileCollection(FIXTURES_DIR)
        assert len(col) >= 2
