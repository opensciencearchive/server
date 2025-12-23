"""Server start/stop commands."""

import sys
import time
from pathlib import Path

import cyclopts

from osa.cli.util import DaemonManager, OSAPaths, ServerStatus

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


@app.command
def logs(
    follow: bool = False,
    lines: int = 50,
) -> None:
    """View server logs.

    Args:
        follow: Follow log output (like tail -f).
        lines: Number of lines to show (default 50). Use 0 for all.
    """
    paths = OSAPaths()
    log_file = paths.server_log

    if not log_file.exists():
        print(f"No log file found at {log_file}", file=sys.stderr)
        print("Has the server been started?", file=sys.stderr)
        sys.exit(1)

    if follow:
        _follow_logs(log_file, lines)
    else:
        _show_logs(log_file, lines)


def _show_logs(log_file: Path, lines: int) -> None:
    """Show the last N lines of the log file."""
    with open(log_file) as f:
        all_lines = f.readlines()

    if lines == 0:
        # Show all lines
        for line in all_lines:
            print(line, end="")
    else:
        # Show last N lines
        for line in all_lines[-lines:]:
            print(line, end="")


def _follow_logs(log_file: Path, initial_lines: int) -> None:
    """Follow log output, similar to tail -f."""
    # First show the last N lines
    _show_logs(log_file, initial_lines)

    # Then follow new content
    try:
        with open(log_file) as f:
            # Seek to end
            f.seek(0, 2)

            while True:
                line = f.readline()
                if line:
                    print(line, end="", flush=True)
                else:
                    time.sleep(0.1)
    except KeyboardInterrupt:
        # Clean exit on Ctrl+C
        print()
