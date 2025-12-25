"""Server start/stop commands."""

import sys
import time
from pathlib import Path

import cyclopts

from osa.cli.console import get_console, relative_time
from osa.cli.util import DaemonManager, OSAPaths, ServerStatus

app = cyclopts.App(name="server", help="Server management commands")


# Local config override (for development)
LOCAL_CONFIG = Path("osa.yaml")


def _resolve_config(paths: OSAPaths, config: Path) -> Path:
    """Resolve config file path.

    Resolution order:
    1. Explicit config argument
    2. ./osa.yaml (local development override)
    3. ~/.config/osa/config.yaml (standard location)

    Raises:
        SystemExit: If no config found or config doesn't exist.
    """
    console = get_console()

    if config is None:
        if LOCAL_CONFIG.exists():
            config = LOCAL_CONFIG
        elif paths.config_file.exists():
            config = paths.config_file
        else:
            console.error(
                "No configuration found",
                hint="Run 'osa init' to set up OSA",
            )
            sys.exit(1)

    if not config.exists():
        console.error(f"Config file not found: {config}")
        sys.exit(1)

    return config


@app.command
def start(
    host: str = "0.0.0.0",
    port: int = 8000,
) -> None:
    """Start the OSA server in the background.

    Args:
        host: Host to bind to.
        port: Port to listen on.
        config: Path to config file. Defaults to ~/.config/osa/config.yaml,
                or ./osa.yaml if present (for local development).
    """
    console = get_console()
    paths = OSAPaths()
    daemon = DaemonManager(paths)

    config = _resolve_config(paths, paths.config_dir)

    try:
        config_path = str(config.resolve())
        info = daemon.start(host=host, port=port, config_file=config_path)
        console.success(f"Server started on http://{host}:{port}")
        console.print(f"  [dim]PID:[/dim] {info.pid}")
        console.print(f"  [dim]Config:[/dim] {config}")
        console.print(f"  [dim]Logs:[/dim] {daemon.paths.server_log}")
    except RuntimeError as e:
        console.error(str(e))
        sys.exit(1)


@app.command
def stop() -> None:
    """Stop the OSA server."""
    console = get_console()
    daemon = DaemonManager()

    try:
        daemon.stop()
        console.success("Server stopped")
    except RuntimeError as e:
        console.error(str(e))
        sys.exit(1)


@app.command
def restart(
    config: Path | None = None,
) -> None:
    """Restart the OSA server (stop + start).

    Args:
        config: Path to config file. If not specified, uses the same resolution
                as 'start' (./osa.yaml or ~/.config/osa/config.yaml).
    """
    console = get_console()
    paths = OSAPaths()
    daemon = DaemonManager(paths)

    # Get current server info before stopping
    info = daemon.status()
    if info.status == ServerStatus.RUNNING:
        host = info.host or "0.0.0.0"
        port = info.port or 8000
        console.print(f"Stopping server (PID {info.pid})...")
        try:
            daemon.stop()
        except RuntimeError:
            pass  # Continue to start even if stop fails
    else:
        host = "0.0.0.0"
        port = 8000
        if info.status == ServerStatus.STALE:
            try:
                daemon.stop()
            except RuntimeError:
                pass

    # Start with resolved config
    start(host=host, port=port)


@app.command
def status() -> None:
    """Check server status."""
    console = get_console()
    daemon = DaemonManager()
    info = daemon.status()

    if info.status == ServerStatus.RUNNING:
        console.success(f"Server running on http://{info.host}:{info.port}")
        console.print(f"  [dim]PID:[/dim] {info.pid}")
        if info.started_at:
            console.print(f"  [dim]Started:[/dim] {relative_time(info.started_at)}")
    elif info.status == ServerStatus.STOPPED:
        console.print("[dim]Server is not running[/dim]")
    elif info.status == ServerStatus.STALE:
        console.warning(f"Stale server state (PID {info.pid} is dead)")
        console.info("Run 'osa server stop' to clean up, or 'osa server start' to start fresh")


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
    console = get_console()
    paths = OSAPaths()
    log_file = paths.server_log

    if not log_file.exists():
        console.error(
            f"No log file found at {log_file}",
            hint="Has the server been started?",
        )
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
        for line in all_lines:
            print(line, end="")
    else:
        for line in all_lines[-lines:]:
            print(line, end="")


def _follow_logs(log_file: Path, initial_lines: int) -> None:
    """Follow log output, similar to tail -f."""
    _show_logs(log_file, initial_lines)

    try:
        with open(log_file) as f:
            f.seek(0, 2)  # Seek to end
            while True:
                line = f.readline()
                if line:
                    print(line, end="", flush=True)
                else:
                    time.sleep(0.1)
    except KeyboardInterrupt:
        print()
