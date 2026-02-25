"""Container entrypoint for the OCI source filesystem contract.

Run as: python -m osa.runtime.source_entrypoint

Flow:
1. Read $OSA_IN/config.json → source config
2. Parse env vars: OSA_SINCE, OSA_LIMIT, OSA_OFFSET
3. Discover source class from _sources registry
4. Create SourceContext(files_dir=$OSA_FILES, output_dir=$OSA_OUT)
5. Call source.pull() → AsyncIterator[SourceRecord]
6. Write each record as a JSON line to $OSA_OUT/records.jsonl
7. If session state set, write $OSA_OUT/session.json
8. Exit 0
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from osa._registry import _sources
from osa.runtime.source_context import SourceContext


def _discover_conventions() -> None:
    """Auto-discover convention packages via entry points."""
    import importlib
    import importlib.metadata

    for ep in importlib.metadata.entry_points(group="osa.conventions"):
        importlib.import_module(ep.value)


async def _run(
    *,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
    files_dir: Path | None = None,
) -> int:
    """Run the source entrypoint. Returns exit code."""
    _discover_conventions()

    if input_dir is None:
        input_dir = Path(os.environ.get("OSA_IN", "/osa/in"))
    if output_dir is None:
        output_dir = Path(os.environ.get("OSA_OUT", "/osa/out"))
    if files_dir is None:
        files_dir = Path(os.environ.get("OSA_FILES", "/osa/files"))

    # Discover source
    if not _sources:
        print("Error: no sources registered", file=sys.stderr)
        return 1

    source_info = _sources[0]
    source_cls = source_info.source_cls

    # Read config
    config = None
    config_path = input_dir / "config.json"
    if config_path.exists():
        try:
            config_data = json.loads(config_path.read_text())
            if hasattr(source_cls, "RuntimeConfig"):
                config = source_cls.RuntimeConfig(**config_data)
            else:
                config = config_data
        except (json.JSONDecodeError, OSError) as exc:
            print(f"Error reading config.json: {exc}", file=sys.stderr)
            return 1

    # Parse env vars
    since: datetime | None = None
    since_str = os.environ.get("OSA_SINCE")
    if since_str:
        since = datetime.fromisoformat(since_str)

    limit: int | None = None
    limit_str = os.environ.get("OSA_LIMIT")
    if limit_str:
        limit = int(limit_str)

    offset = int(os.environ.get("OSA_OFFSET", "0"))

    # Read session from input if available
    session = None
    session_path = input_dir / "session.json"
    if session_path.exists():
        try:
            session = json.loads(session_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass

    # Instantiate source
    if config is not None:
        source = source_cls(config)
    else:
        source = source_cls()

    # Create context
    files_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    ctx = SourceContext(files_dir=files_dir, output_dir=output_dir)

    try:
        # Write records.jsonl
        records_path = output_dir / "records.jsonl"
        count = 0
        with records_path.open("w") as f:
            async for record in source.pull(
                ctx=ctx,
                since=since,
                limit=limit,
                offset=offset,
                session=session,
            ):
                f.write(record.model_dump_json() + "\n")
                count += 1

        # Write session if set
        ctx.write_session()

        print(f"Wrote {count} records to records.jsonl", file=sys.stderr)
        return 0

    except Exception as exc:
        print(f"Error during source pull: {exc}", file=sys.stderr)
        return 1
    finally:
        await ctx.close()


def run_source_entrypoint(
    *,
    input_dir: Path | None = None,
    output_dir: Path | None = None,
    files_dir: Path | None = None,
) -> int:
    """Synchronous wrapper for the source entrypoint."""
    return asyncio.run(
        _run(input_dir=input_dir, output_dir=output_dir, files_dir=files_dir)
    )


def main() -> None:
    """Console script entry point for osa-run-source."""
    sys.exit(run_source_entrypoint())


if __name__ == "__main__":
    main()
