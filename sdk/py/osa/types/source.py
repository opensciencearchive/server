"""Source types for SDK — SourceFileRef and SourceRecord."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class SourceSchedule(BaseModel, frozen=True):
    """Cron schedule for periodic source runs."""

    cron: str
    limit: int | None = None


class InitialRun(BaseModel, frozen=True):
    """Configuration for the first source run on server startup."""

    limit: int | None = None


class SourceFileRef(BaseModel, frozen=True):
    """A reference to a file written by a source container.

    The source writes files to $OSA_FILES/{source_id}/{name}.
    The server renames this directory into the deposition's canonical location.
    """

    name: str  # e.g. "structure.cif"
    relative_path: str  # e.g. "{source_id}/structure.cif" (relative to $OSA_FILES)


class SourceRecord(BaseModel, frozen=True):
    """A record produced by a source container, written to records.jsonl."""

    source_id: str
    metadata: dict[str, Any]
    files: list[SourceFileRef] = []
    fetched_at: datetime | None = None
