"""Server state file I/O utilities.

Handles reading and writing the server.json state file that tracks
the running daemon's PID, host, port, and start time.
"""

import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from osa.cli.util.paths import ServerState


def read_server_state(state_file: Path) -> ServerState | None:
    """Read server state from file.

    Args:
        state_file: Path to the server.json file.

    Returns:
        ServerState if file exists and is valid, None otherwise.
    """
    if not state_file.exists():
        return None
    try:
        data = json.loads(state_file.read_text())
        return ServerState(**data)
    except (json.JSONDecodeError, TypeError, KeyError, OSError):
        return None


def write_server_state(state_file: Path, pid: int, host: str, port: int) -> ServerState:
    """Write server state to file.

    Creates parent directories if they don't exist.

    Args:
        state_file: Path to the server.json file.
        pid: Process ID of the running server.
        host: Host the server is listening on.
        port: Port the server is listening on.

    Returns:
        The created ServerState.
    """
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state = ServerState(
        pid=pid,
        host=host,
        port=port,
        started_at=datetime.now(UTC).isoformat(),
    )
    state_file.write_text(json.dumps(asdict(state), indent=2))
    return state


def remove_server_state(state_file: Path) -> None:
    """Remove server state file.

    Args:
        state_file: Path to the server.json file.
    """
    if state_file.exists():
        state_file.unlink()
