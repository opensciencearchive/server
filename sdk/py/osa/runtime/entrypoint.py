"""Container entrypoint for the OCI filesystem contract."""

from __future__ import annotations

import json
import os
import sys
import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from osa._registry import _hooks
from osa.authoring.validator import Reject
from osa.types.files import FileCollection
from osa.types.record import Record
from osa.types.schema import MetadataSchema


def _get_schema_type(fn: Callable[..., Any]) -> type[MetadataSchema]:
    """Look up the schema type for a hook function from the registry."""
    for info in _hooks:
        if info.fn is fn:
            return info.schema_type
    msg = f"Function {fn.__name__} is not a registered hook"
    raise ValueError(msg)


def run_hook_entrypoint(
    *,
    hook_fn: Callable[..., Any],
    input_dir: Path | None = None,
    output_dir: Path | None = None,
) -> int:
    """Run a single hook against the OCI filesystem contract.

    Reads ``record.json`` and ``files/`` from the input directory,
    runs the hook, and writes ``features.json`` to the output directory.
    On Reject, writes rejection to ``progress.jsonl`` and returns 0.

    Returns 0 on successful execution, non-zero on unhandled errors.
    """
    if input_dir is None:
        input_dir = Path(os.environ.get("OSA_IN", "/osa/in"))
    if output_dir is None:
        output_dir = Path(os.environ.get("OSA_OUT", "/osa/out"))

    if not input_dir.exists():
        print(f"Error: input directory not found: {input_dir}", file=sys.stderr)
        return 1

    record_path = input_dir / "record.json"
    if not record_path.exists():
        print(f"Error: record.json not found in {input_dir}", file=sys.stderr)
        return 1

    try:
        meta_dict = json.loads(record_path.read_text())
    except (json.JSONDecodeError, OSError) as exc:
        print(f"Error reading record.json: {exc}", file=sys.stderr)
        return 1

    # Build file collection
    files_dir = input_dir / "files"
    if files_dir.is_dir():
        file_collection = FileCollection(files_dir)
    else:
        import tempfile

        file_collection = FileCollection(Path(tempfile.mkdtemp()))

    # Build record
    schema_type = _get_schema_type(hook_fn)
    metadata = schema_type(**meta_dict)
    record: Record = Record(
        id=str(uuid.uuid4()),
        created_at=datetime.now(),
        metadata=metadata,
        files=file_collection,
    )

    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        result = hook_fn(record)
    except Reject as e:
        # Write rejection to progress.jsonl
        entry = {"status": "rejected", "reason": str(e)}
        with (output_dir / "progress.jsonl").open("a") as f:
            f.write(json.dumps(entry) + "\n")
        return 0

    # Write features.json
    if isinstance(result, list):
        features = [
            item.model_dump() if isinstance(item, BaseModel) else item
            for item in result
        ]
    elif isinstance(result, BaseModel):
        features = result.model_dump()
    else:
        features = result

    (output_dir / "features.json").write_text(json.dumps(features, indent=2))
    return 0
