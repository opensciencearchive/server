"""Test harness for running hooks in-process."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from osa._registry import _hooks
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


def _build_record(
    fn: Callable[..., Any],
    meta: dict[str, Any] | MetadataSchema,
    files: Path | None,
    srn: str | None = None,
) -> Record[Any]:
    """Construct a Record[T] for testing from a decorated function."""
    schema_type = _get_schema_type(fn)

    if isinstance(meta, dict):
        metadata = schema_type(**meta)
    else:
        metadata = meta

    if files is not None:
        file_collection = FileCollection(files)
    else:
        import tempfile

        file_collection = FileCollection(Path(tempfile.mkdtemp()))

    return Record(
        id=str(uuid.uuid4()),
        created_at=datetime.now(),
        metadata=metadata,
        files=file_collection,
        srn=srn or "",
    )


def run_hook(
    fn: Callable[..., Any],
    *,
    meta: dict[str, Any] | MetadataSchema,
    files: Path | None = None,
    srn: str | None = None,
) -> Any:
    """Run a hook function in-process for testing.

    Constructs a :class:`Record[T]` from the provided metadata and
    optional files directory, then executes the hook.
    """
    record = _build_record(fn, meta, files, srn=srn)
    return fn(record)
