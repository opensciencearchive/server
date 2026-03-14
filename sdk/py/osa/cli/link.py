"""Per-directory project linking for OSA CLI.

Stores server URL in .osa/config.json so commands don't need --server every time.
Resolution chain: --server flag → OSA_SERVER env → .osa/config.json → error.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def write_link(server: str, *, project_dir: Path | None = None) -> Path:
    """Write .osa/config.json in project_dir (default: cwd).

    Returns path to the config file.
    """
    project_dir = project_dir or Path.cwd()
    server = server.rstrip("/")

    config_dir = project_dir / ".osa"
    config_dir.mkdir(parents=True, exist_ok=True)

    config_path = config_dir / "config.json"
    config_path.write_text(json.dumps({"server": server}, indent=2) + "\n")

    return config_path


def read_link(*, project_dir: Path | None = None) -> str | None:
    """Read server URL from .osa/config.json.

    Returns the server URL or None if not found/invalid.
    """
    project_dir = project_dir or Path.cwd()
    config_path = project_dir / ".osa" / "config.json"

    if not config_path.exists():
        return None

    try:
        data = json.loads(config_path.read_text())
        server = data.get("server")
        if isinstance(server, str) and server:
            return server
        return None
    except (json.JSONDecodeError, OSError):
        return None


def resolve_server(*, flag: str | None = None, project_dir: Path | None = None) -> str:
    """Resolve server URL: --server flag → OSA_SERVER env → .osa/config.json → error."""
    if flag:
        return flag.rstrip("/")

    env = os.environ.get("OSA_SERVER")
    if env:
        return env.rstrip("/")

    linked = read_link(project_dir=project_dir)
    if linked:
        return linked

    print(
        "Error: No server specified. Use --server <url>, set OSA_SERVER, "
        "or run `osa link --server <url>` in your project directory.",
        file=sys.stderr,
    )
    sys.exit(1)
