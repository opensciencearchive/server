"""Manages the ~/.osa directory structure."""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass
class ServerState:
    """Persisted server state."""

    pid: int
    host: str
    port: int
    started_at: str  # ISO format


class OSAPaths:
    """Manages paths within the ~/.osa directory.

    Directory structure:
        ~/.osa/
            server.json     # Running server state (PID, host, port, etc.)
            logs/
                server.log  # Server output logs
            data/
                vectors/    # ChromaDB persistence
    """

    def __init__(self, base_dir: Path | None = None) -> None:
        """Initialize paths.

        Args:
            base_dir: Override base directory (default: ~/.osa).
                      Useful for testing.
        """
        self._base = base_dir or Path.home() / ".osa"

    @property
    def base(self) -> Path:
        """Base directory (~/.osa)."""
        return self._base

    @property
    def server_state_file(self) -> Path:
        """Server state file."""
        return self._base / "server.json"

    @property
    def logs_dir(self) -> Path:
        """Logs directory."""
        return self._base / "logs"

    @property
    def server_log(self) -> Path:
        """Server log file."""
        return self.logs_dir / "server.log"

    @property
    def data_dir(self) -> Path:
        """Data directory."""
        return self._base / "data"

    @property
    def vectors_dir(self) -> Path:
        """Vector database directory."""
        return self.data_dir / "vectors"

    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        self._base.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.vectors_dir.mkdir(parents=True, exist_ok=True)

    def read_server_state(self) -> ServerState | None:
        """Read server state from file.

        Returns:
            ServerState if file exists and is valid, None otherwise.
        """
        if not self.server_state_file.exists():
            return None
        try:
            data = json.loads(self.server_state_file.read_text())
            return ServerState(**data)
        except (json.JSONDecodeError, TypeError, KeyError, OSError):
            return None

    def write_server_state(self, pid: int, host: str, port: int) -> ServerState:
        """Write server state to file."""
        self.ensure_directories()
        state = ServerState(
            pid=pid,
            host=host,
            port=port,
            started_at=datetime.now(UTC).isoformat(),
        )
        self.server_state_file.write_text(json.dumps(asdict(state), indent=2))
        return state

    def remove_server_state(self) -> None:
        """Remove server state file."""
        if self.server_state_file.exists():
            self.server_state_file.unlink()
