"""Manages OSA directory structure.

Supports two modes:

1. **Unified mode** (OSA_DATA_DIR set): All data under a single directory
   for container deployments with single volume mount.

   OSA_DATA_DIR=/data
     /data/config/     → config files
     /data/data/       → database, vectors
     /data/state/      → logs, server state
     /data/cache/      → CLI cache

2. **XDG mode** (default): Follows XDG Base Directory specification
   for local development.

   ~/.config/osa/      → config files
   ~/.local/share/osa/ → database, vectors
   ~/.local/state/osa/ → logs, server state
   ~/.cache/osa/       → CLI cache
"""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class ServerState:
    """Persisted server state."""

    pid: int
    host: str
    port: int
    started_at: str  # ISO format


class OSAPaths:
    """Computes OSA paths for unified or XDG mode.

    This class is purely for path computation - no file I/O operations.
    Use server_state.py and search_cache.py for reading/writing files.
    """

    def __init__(self, *, unified_data_dir: Path | None = None) -> None:
        """Initialize paths.

        Args:
            unified_data_dir: If set, derive all paths from this directory
                (container mode). If None, use XDG defaults (local mode).
        """
        if unified_data_dir is not None:
            # Unified mode: all paths under single directory
            self._config_dir = unified_data_dir / "config"
            self._data_dir = unified_data_dir / "data"
            self._state_dir = unified_data_dir / "state"
            self._cache_dir = unified_data_dir / "cache"
        else:
            # XDG mode: standard paths for local development
            home = Path.home()
            self._config_dir = home / ".config" / "osa"
            self._data_dir = home / ".local" / "share" / "osa"
            self._state_dir = home / ".local" / "state" / "osa"
            self._cache_dir = home / ".cache" / "osa"

    # -------------------------------------------------------------------------
    # Base directories
    # -------------------------------------------------------------------------

    @property
    def config_dir(self) -> Path:
        """Config directory (~/.config/osa)."""
        return self._config_dir

    @property
    def data_dir(self) -> Path:
        """Data directory (~/.local/share/osa)."""
        return self._data_dir

    @property
    def state_dir(self) -> Path:
        """State directory (~/.local/state/osa)."""
        return self._state_dir

    @property
    def cache_dir(self) -> Path:
        """Cache directory (~/.cache/osa)."""
        return self._cache_dir

    # -------------------------------------------------------------------------
    # Config paths
    # -------------------------------------------------------------------------

    @property
    def config_file(self) -> Path:
        """Main config file."""
        return self._config_dir / "config.yaml"

    # -------------------------------------------------------------------------
    # Data paths
    # -------------------------------------------------------------------------

    @property
    def database_file(self) -> Path:
        """SQLite database file."""
        return self._data_dir / "osa.db"

    @property
    def vectors_dir(self) -> Path:
        """Vector database directory."""
        return self._data_dir / "vectors"

    # -------------------------------------------------------------------------
    # State paths
    # -------------------------------------------------------------------------

    @property
    def server_state_file(self) -> Path:
        """Server state file."""
        return self._state_dir / "server.json"

    @property
    def logs_dir(self) -> Path:
        """Logs directory."""
        return self._state_dir / "logs"

    @property
    def server_log(self) -> Path:
        """Server log file."""
        return self.logs_dir / "server.log"

    # -------------------------------------------------------------------------
    # Cache paths
    # -------------------------------------------------------------------------

    @property
    def search_cache_file(self) -> Path:
        """Search results cache file."""
        return self._cache_dir / "last_search.json"

    # -------------------------------------------------------------------------
    # Directory management
    # -------------------------------------------------------------------------

    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        self._config_dir.mkdir(parents=True, exist_ok=True)
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._state_dir.mkdir(parents=True, exist_ok=True)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.vectors_dir.mkdir(parents=True, exist_ok=True)

    def is_initialized(self) -> bool:
        """Check if OSA has been initialized (config file exists)."""
        return self.config_file.exists()
