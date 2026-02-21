"""File access abstractions for OSA records."""

from __future__ import annotations
from typing import List

import fnmatch
from collections.abc import Iterator
from pathlib import Path


class File:
    """A single data file within a deposition."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        """Absolute path to the file."""
        return self._path

    @property
    def name(self) -> str:
        """Filename string."""
        return self._path.name

    @property
    def size(self) -> int:
        """File size in bytes."""
        return self._path.stat().st_size

    def read(self) -> bytes:
        """Read and return the full file contents."""
        return self._path.read_bytes()


class FileCollection:
    """Ordered collection of files associated with a record."""

    def __init__(self, directory: Path) -> None:
        self._files = sorted(
            (File(p) for p in directory.iterdir() if p.is_file()),
            key=lambda f: f.name,
        )

    def list(self) -> List[File]:
        """Return all files."""
        return list(self._files)

    def glob(self, pattern: str) -> List[File]:
        """Filter files by glob pattern on filenames."""
        return [f for f in self._files if fnmatch.fnmatch(f.name, pattern)]

    def __getitem__(self, name: str) -> File:
        """Access a file by name. Raises KeyError if not found."""
        for f in self._files:
            if f.name == name:
                return f
        raise KeyError(name)

    def __iter__(self) -> Iterator[File]:
        return iter(self._files)

    def __len__(self) -> int:
        return len(self._files)
