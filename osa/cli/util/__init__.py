"""CLI utilities (paths, daemon management, caching)."""

from osa.cli.util.daemon import ConfigError, DaemonManager, ServerInfo, ServerStatus
from osa.cli.util.paths import OSAPaths, ServerState
from osa.cli.util.search_cache import read_search_cache, write_search_cache
from osa.cli.util.server_state import (
    read_server_state,
    remove_server_state,
    write_server_state,
)

__all__ = [
    "ConfigError",
    "DaemonManager",
    "OSAPaths",
    "ServerInfo",
    "ServerState",
    "ServerStatus",
    # Server state I/O
    "read_server_state",
    "write_server_state",
    "remove_server_state",
    # Search cache I/O
    "read_search_cache",
    "write_search_cache",
]
