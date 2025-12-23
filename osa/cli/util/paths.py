"""Manages OSA directory structure following XDG Base Directory spec.

Directory layout:
    ~/.config/osa/
        config.yaml         # User configuration

    ~/.local/share/osa/
        osa.db              # SQLite database
        vectors/            # Vector index (ChromaDB)

    ~/.local/state/osa/
        server.json         # Daemon state (PID, host, port)
        logs/
            server.log      # Server logs

    ~/.cache/osa/
        last_search.json    # CLI search cache
"""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from osa.cli.models import SearchCache, SearchHit


@dataclass
class ServerState:
    """Persisted server state."""

    pid: int
    host: str
    port: int
    started_at: str  # ISO format


class OSAPaths:
    """Manages OSA paths following XDG Base Directory specification.

    Supports overriding individual directories for testing.
    """

    def __init__(
        self,
        *,
        config_dir: Path | None = None,
        data_dir: Path | None = None,
        state_dir: Path | None = None,
        cache_dir: Path | None = None,
    ) -> None:
        """Initialize paths.

        Args:
            config_dir: Override config directory (default: ~/.config/osa).
            data_dir: Override data directory (default: ~/.local/share/osa).
            state_dir: Override state directory (default: ~/.local/state/osa).
            cache_dir: Override cache directory (default: ~/.cache/osa).
        """
        home = Path.home()
        self._config_dir = config_dir or home / ".config" / "osa"
        self._data_dir = data_dir or home / ".local" / "share" / "osa"
        self._state_dir = state_dir or home / ".local" / "state" / "osa"
        self._cache_dir = cache_dir or home / ".cache" / "osa"

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

    # -------------------------------------------------------------------------
    # Server state
    # -------------------------------------------------------------------------

    def read_server_state(self) -> ServerState | None:
        """Read server state from file."""
        if not self.server_state_file.exists():
            return None
        try:
            data = json.loads(self.server_state_file.read_text())
            return ServerState(**data)
        except (json.JSONDecodeError, TypeError, KeyError, OSError):
            return None

    def write_server_state(self, pid: int, host: str, port: int) -> ServerState:
        """Write server state to file."""
        self._state_dir.mkdir(parents=True, exist_ok=True)
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

    # -------------------------------------------------------------------------
    # Search cache
    # -------------------------------------------------------------------------

    def read_search_cache(self) -> SearchCache | None:
        """Read cached search results."""
        if not self.search_cache_file.exists():
            return None
        try:
            data = json.loads(self.search_cache_file.read_text())
            return SearchCache.model_validate(data)
        except (json.JSONDecodeError, ValidationError, OSError):
            return None

    def write_search_cache(
        self,
        index: str,
        query: str,
        results: list[SearchHit],
    ) -> SearchCache:
        """Write search results to cache for numbered lookup."""
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        cache = SearchCache(
            index=index,
            query=query,
            searched_at=datetime.now(UTC).isoformat(),
            results=results,
        )
        self.search_cache_file.write_text(cache.model_dump_json(indent=2))
        return cache
