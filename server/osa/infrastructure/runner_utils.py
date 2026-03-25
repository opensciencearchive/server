"""Shared result-parsing utilities for OCI and K8s runners."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from osa.domain.validation.model.hook_result import ProgressEntry

if TYPE_CHECKING:
    from osa.infrastructure.s3.client import S3Client


def parse_progress_file(output_dir: Path) -> list[ProgressEntry]:
    """Parse progress.jsonl from hook output directory."""
    progress_file = output_dir / "progress.jsonl"
    if not progress_file.exists():
        return []

    entries = []
    for line in progress_file.read_text().strip().split("\n"):
        if not line.strip():
            continue
        try:
            data = json.loads(line)
            entries.append(
                ProgressEntry(
                    step=data.get("step"),
                    status=data.get("status", "unknown"),
                    message=data.get("message"),
                )
            )
        except json.JSONDecodeError:
            continue
    return entries


def detect_rejection(progress: list[ProgressEntry]) -> tuple[bool, str | None]:
    """Check if any progress entry indicates rejection.

    Returns (is_rejected, rejection_reason).
    """
    for entry in reversed(progress):
        if entry.status == "rejected":
            return True, entry.message
    return False, None


def parse_memory(memory: str) -> int:
    """Parse memory string like '2g' or '512m' to bytes."""
    memory = memory.strip().lower()
    match = re.match(r"^(\d+(?:\.\d+)?)(g|m|k)?i?$", memory)
    if not match:
        raise ValueError(f"Invalid memory format: {memory}")

    amount = float(match.group(1))
    unit = match.group(2)

    match unit:
        case "g":
            return int(amount * 1024 * 1024 * 1024)
        case "m":
            return int(amount * 1024 * 1024)
        case "k":
            return int(amount * 1024)
        case None:
            return int(amount)
        case _:
            raise ValueError(f"Unknown memory unit: {unit}")


_MEMORY_RE = re.compile(r"^(\d+(?:\.\d+)?)(g|m|k)?i?$")


def to_k8s_quantity(memory: str) -> str:
    """Convert a Docker-style memory string to a K8s resource quantity.

    Docker uses lowercase units where 'm' means megabytes.
    K8s uses IEC binary units where 'Mi' means mebibytes and lowercase 'm'
    means *milli* (10⁻³).  This function bridges the two conventions.

    Fractional values are converted down one unit to produce an integer
    quantity (e.g. "1.5g" → "1536Mi") since K8s quantities must be integers
    when using binary suffixes.
    """
    raw = memory.strip().lower()
    match = _MEMORY_RE.match(raw)
    if not match:
        raise ValueError(f"Invalid memory format: {memory}")

    amount = float(match.group(1))
    unit = match.group(2)

    match unit:
        case "g":
            if amount == int(amount):
                return f"{int(amount)}Gi"
            return f"{int(amount * 1024)}Mi"
        case "m":
            if amount == int(amount):
                return f"{int(amount)}Mi"
            return f"{int(amount * 1024)}Ki"
        case "k":
            return f"{int(amount)}Ki"
        case None:
            return str(int(amount))
        case _:
            raise ValueError(f"Unknown memory unit: {unit}")


def relative_path(path: Path, data_mount_path: str) -> str:
    """Strip the data mount prefix to get a PVC-relative subpath.

    Used by K8s runners to convert absolute paths into PVC sub_path values.
    """
    mount = data_mount_path.rstrip("/")
    path_str = str(path)
    if not path_str.startswith(mount):
        raise ValueError(f"Path {path} is outside the data mount prefix {mount}")
    return path_str[len(mount) :].lstrip("/")


def parse_records_file(output_dir: Path) -> list[dict[str, Any]]:
    """Parse records.jsonl from ingester output directory."""
    import logfire

    records: list[dict[str, Any]] = []
    records_file = output_dir / "records.jsonl"
    if not records_file.exists():
        return records

    for line in records_file.read_text().strip().split("\n"):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            logfire.warn("Skipping invalid JSON line in records.jsonl")
            continue
    return records


def parse_session_file(output_dir: Path) -> dict[str, Any] | None:
    """Parse session.json from source output directory."""
    import logfire

    session_file = output_dir / "session.json"
    if not session_file.exists():
        return None
    try:
        return json.loads(session_file.read_text())
    except json.JSONDecodeError:
        logfire.warn("Invalid session.json")
        return None


# ── S3-based parse functions (used by K8s runners) ──────────────────


async def parse_progress_from_s3(s3: S3Client, prefix: str) -> list[ProgressEntry]:
    """Parse progress.jsonl from S3 key prefix."""
    import logfire

    key = f"{prefix}/progress.jsonl"
    try:
        data = await s3.get_object(key)
    except Exception:
        return []

    entries = []
    for line in data.decode().strip().split("\n"):
        if not line.strip():
            continue
        try:
            d = json.loads(line)
            entries.append(
                ProgressEntry(
                    step=d.get("step"),
                    status=d.get("status", "unknown"),
                    message=d.get("message"),
                )
            )
        except json.JSONDecodeError:
            logfire.warn("Skipping invalid JSON line in progress.jsonl")
            continue
    return entries


async def parse_records_from_s3(s3: S3Client, prefix: str) -> list[dict[str, Any]]:
    """Parse records.jsonl from S3 key prefix."""
    import logfire

    key = f"{prefix}/records.jsonl"
    try:
        data = await s3.get_object(key)
    except Exception:
        return []

    records: list[dict[str, Any]] = []
    for line in data.decode().strip().split("\n"):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            logfire.warn("Skipping invalid JSON line in records.jsonl")
            continue
    return records


async def parse_session_from_s3(s3: S3Client, prefix: str) -> dict[str, Any] | None:
    """Parse session.json from S3 key prefix."""
    import logfire

    key = f"{prefix}/session.json"
    try:
        data = await s3.get_object(key)
    except Exception:
        return None
    try:
        return json.loads(data)
    except json.JSONDecodeError:
        logfire.warn("Invalid session.json from S3")
        return None
