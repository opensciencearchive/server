"""Server start/stop commands."""

import sys
from pathlib import Path

import cyclopts

from osa.infrastructure.local import DaemonManager, ServerStatus

app = cyclopts.App(name="server", help="Server management commands")


DEFAULT_CONFIG = Path("osa.yaml")


@app.command
def start(
    host: str = "0.0.0.0",
    port: int = 8000,
    config: Path | None = None,
) -> None:
    """Start the OSA server in the background.

    Args:
        host: Host to bind to.
        port: Port to listen on.
        config: Path to YAML config file. Defaults to ./osa.yaml if it exists.
    """
    daemon = DaemonManager()

    # Auto-detect osa.yaml in current directory if no config specified
    if config is None and DEFAULT_CONFIG.exists():
        config = DEFAULT_CONFIG

    # Validate config file exists if provided
    if config and not config.exists():
        print(f"Error: config file not found: {config}", file=sys.stderr)
        sys.exit(1)

    try:
        config_path = str(config.resolve()) if config else None
        info = daemon.start(host=host, port=port, config_file=config_path)
        print(f"Server started on http://{host}:{port} (PID {info.pid})")
        if config:
            print(f"Config: {config}")
        print(f"Logs: {daemon.paths.server_log}")
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


@app.command
def stop() -> None:
    """Stop the OSA server."""
    daemon = DaemonManager()

    try:
        daemon.stop()
        print("Server stopped")
    except RuntimeError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


@app.command
def status() -> None:
    """Check server status."""
    daemon = DaemonManager()
    info = daemon.status()

    if info.status == ServerStatus.RUNNING:
        print(f"Server is running on http://{info.host}:{info.port} (PID {info.pid})")
        if info.started_at:
            print(f"Started at: {info.started_at}")
    elif info.status == ServerStatus.STOPPED:
        print("Server is not running")
    elif info.status == ServerStatus.STALE:
        print(f"Server has stale state (PID {info.pid} is dead)")
        print("Run 'osa server stop' to clean up, or 'osa server start' to start fresh")
