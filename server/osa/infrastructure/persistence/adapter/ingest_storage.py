"""Filesystem-backed ingest storage adapter for local and Docker deployments."""

import json
import os
from pathlib import Path
from typing import Any

from osa.infrastructure.storage.layout import StorageLayout


class FilesystemIngestStorage:
    """Local filesystem adapter for IngestStoragePort.

    Used in local dev and self-hosted (Docker) deployments.
    Delegates path computation to StorageLayout.
    """

    def __init__(self, layout: StorageLayout) -> None:
        self._layout = layout

    async def read_session(self, ingest_run_srn: str) -> dict[str, Any] | None:
        session_file = self._layout.ingest_session_file(ingest_run_srn)
        if not session_file.exists():
            return None
        return json.loads(session_file.read_text())

    async def write_session(self, ingest_run_srn: str, session: dict[str, Any]) -> None:
        session_file = self._layout.ingest_session_file(ingest_run_srn)
        session_file.parent.mkdir(parents=True, exist_ok=True)
        # Atomic write via temp file + os.replace to handle mountpoint-for-s3
        tmp = session_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(session))
        os.replace(tmp, session_file)

    async def write_records(
        self, ingest_run_srn: str, batch_index: int, records: list[dict[str, Any]]
    ) -> None:
        ingester_dir = self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index)
        ingester_dir.mkdir(parents=True, exist_ok=True)
        records_file = ingester_dir / "records.jsonl"
        with records_file.open("w") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")

    async def read_records(self, ingest_run_srn: str, batch_index: int) -> list[dict[str, Any]]:
        ingester_dir = self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index)
        records_file = ingester_dir / "records.jsonl"
        if not records_file.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in records_file.open():
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
        return records

    def batch_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        d = self._layout.ingest_batch_dir(ingest_run_srn, batch_index)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def batch_work_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        d = self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index)
        d.mkdir(parents=True, exist_ok=True)
        return d

    def batch_files_dir(self, ingest_run_srn: str, batch_index: int) -> Path:
        d = self._layout.ingest_batch_ingester_dir(ingest_run_srn, batch_index) / "files"
        d.mkdir(parents=True, exist_ok=True)
        return d

    def hook_work_dir(self, ingest_run_srn: str, batch_index: int, hook_name: str) -> Path:
        d = self._layout.ingest_batch_hook_dir(ingest_run_srn, batch_index, hook_name)
        d.mkdir(parents=True, exist_ok=True)
        return d
