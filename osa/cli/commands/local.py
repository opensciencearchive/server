"""Local development server commands (start/stop/logs/status/clean)."""

import shutil
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import cyclopts

from osa.cli.console import get_console, relative_time

if TYPE_CHECKING:
    from osa.cli.console import Console
from osa.cli.util import ConfigError, DaemonManager, OSAPaths, ServerStatus

app = cyclopts.App(name="local", help="Local development server")

# Local config override (for development)
LOCAL_CONFIG = Path("osa.yaml")

# Available templates
Template = Literal["geo", "minimal"]

GEO_TEMPLATE = """\
# OSA Configuration - GEO Template

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default, stored in $OSA_DATA_DIR or ~/.local/share/osa/)
# database:
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

# GEO Source - pulls from NCBI Gene Expression Omnibus via Entrez API
sources:
  - source: geo-entrez
    config:
      record_type: gse  # gse (~250k all) or gds (~5k curated)
      email: your@email.com  # Required by NCBI - please update this
      # api_key: null  # Optional: NCBI API key for higher rate limits (https://account.ncbi.nlm.nih.gov/settings/)
    initial_run:
      enabled: true
      limit: 50
    # schedule:
    #   cron: "0 * * * *"  # Hourly
    #   limit: 100

# Vector search index
indexes:
  - name: vector
    backend: vector
    config:
      persist_dir: {vectors_dir}
      embedding:
        model: all-MiniLM-L6-v2
        fields: [title, summary, organism, platform, entry_type]
"""

MINIMAL_TEMPLATE = """\
# OSA Configuration

server:
  name: "My OSA Node"
  domain: "localhost"

# Database (SQLite by default, stored in $OSA_DATA_DIR or ~/.local/share/osa/)
# database:
#   auto_migrate: true

# Logging
# logging:
#   level: "INFO"

# Add your sources here:
# sources:
#   - source: geo-entrez
#     config:
#       email: your@email.com

# Add your indexes here:
# indexes:
#   - name: my-index
#     backend: vector
#     config:
#       persist_dir: {vectors_dir}
"""


def _get_template_content(template: Template, paths: OSAPaths) -> str:
    """Get template content with paths substituted."""
    if template == "geo":
        return GEO_TEMPLATE.format(vectors_dir=paths.vectors_dir)
    else:
        return MINIMAL_TEMPLATE.format(vectors_dir=paths.vectors_dir)


def _resolve_config(paths: OSAPaths, console: "Console") -> Path:
    """Resolve config file path.

    Resolution order:
    1. ./osa.yaml (local development override)
    2. $OSA_DATA_DIR/config/config.yaml (unified mode)
    3. ~/.config/osa/config.yaml (XDG standard location)

    Returns:
        Path to config file.

    Exits:
        If no config found, prints helpful message and exits.
    """
    if LOCAL_CONFIG.exists():
        return LOCAL_CONFIG
    elif paths.config_file.exists():
        return paths.config_file
    else:
        console.error("No configuration found")
        console.print()
        console.print("Create a config file first:")
        console.print("  [bold]osa local init geo[/bold]      # GEO template with vector search")
        console.print("  [bold]osa local init minimal[/bold]  # Blank template")
        console.print()
        console.print("Then run: [bold]osa local start[/bold]")
        sys.exit(1)


@app.command
def start(
    host: str = "0.0.0.0",
    port: int = 8000,
) -> None:
    """Start the OSA server in the background.

    Args:
        host: Host to bind to.
        port: Port to listen on.
    """
    console = get_console()
    paths = OSAPaths()

    # Ensure directories exist
    paths.ensure_directories()

    # Resolve config (exits if not found)
    config = _resolve_config(paths, console)

    daemon = DaemonManager(paths)

    try:
        config_path = str(config.resolve())
        info = daemon.start(host=host, port=port, config_file=config_path)
        console.success(f"Server started on http://{host}:{port}")
        console.print(f"  [dim]PID:[/dim] {info.pid}")
        console.print(f"  [dim]Config:[/dim] {config}")
        console.print(f"  [dim]Logs:[/dim] {daemon.paths.server_log}")
    except ConfigError as e:
        console.error(e.message)
        for detail in e.details:
            console.print(f"  [dim]•[/dim] {detail}")
        console.print()
        if config:
            console.info(f"Config file: {config}")
        sys.exit(1)
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
def restart() -> None:
    """Restart the OSA server (stop + start)."""
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
        console.info("Run 'osa local stop' to clean up, or 'osa local start' to start fresh")


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


@app.command
def clean(
    force: bool = False,
    keep_config: bool = False,
    keep_logs: bool = False,
) -> None:
    """Wipe OSA data directories to start fresh.

    Stops the server if running, then removes:
    - Data directory (database, vectors)
    - State directory (server state, logs)
    - Cache directory (search cache)
    - Config directory (unless --keep-config)

    Args:
        force: Skip confirmation prompt.
        keep_config: Keep the config directory.
        keep_logs: Keep the logs directory.
    """
    console = get_console()
    paths = OSAPaths()

    # Check which directories exist
    dirs_to_clean: list[tuple[str, Path]] = []
    if paths.data_dir.exists():
        dirs_to_clean.append(("Data", paths.data_dir))
    if paths.state_dir.exists():
        dirs_to_clean.append(("State", paths.state_dir))
    if paths.cache_dir.exists():
        dirs_to_clean.append(("Cache", paths.cache_dir))
    if paths.config_dir.exists() and not keep_config:
        dirs_to_clean.append(("Config", paths.config_dir))

    if not dirs_to_clean:
        console.info("Nothing to clean - no OSA directories exist")
        return

    # Check if server is running
    daemon = DaemonManager()
    status_info = daemon.status()

    if status_info.status == ServerStatus.RUNNING:
        if not force:
            console.warning(f"Server is running (PID {status_info.pid})")
            response = input("Stop server and clean? [y/N] ").strip().lower()
            if response != "y":
                console.info("Aborted")
                sys.exit(1)
        console.info("Stopping server...")
        daemon.stop()

    # Confirm before wiping
    if not force:
        console.print("[bold]This will delete:[/bold]")
        for name, path in dirs_to_clean:
            console.print(f"  [cyan]{path}[/cyan]  [dim]({name.lower()})[/dim]")
        if keep_logs:
            console.print(f"  [dim](keeping logs at {paths.logs_dir})[/dim]")
        if keep_config and paths.config_dir.exists():
            console.print(f"  [dim](keeping config at {paths.config_dir})[/dim]")
        console.print()
        response = input("Are you sure? [y/N] ").strip().lower()
        if response != "y":
            console.info("Aborted")
            sys.exit(1)

    # Perform cleanup
    for name, path in dirs_to_clean:
        if name == "State" and keep_logs:
            # Delete everything in state except logs
            for item in path.iterdir():
                if item == paths.logs_dir:
                    continue
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
            console.success(f"Cleaned {path} (logs preserved)")
        else:
            shutil.rmtree(path)
            console.success(f"Removed {path}")

    console.print()
    console.info("Run 'osa local init' to set up OSA again")


@app.command
def init(
    template: Template | None = None,
    /,
    force: bool = False,
) -> None:
    """Initialize OSA for local development.

    Creates directories and config file in XDG locations (or $OSA_DATA_DIR).
    Edit the config to customize your instance, then run 'osa local start'.

    Args:
        template: Template to use (geo, minimal).
        force: Overwrite existing configuration.
    """
    console = get_console()
    paths = OSAPaths()

    # If no template specified, show available options
    if template is None:
        console.print("[bold]Available templates:[/bold]\n")
        console.print("  [cyan]geo[/cyan]      NCBI GEO integration with vector search")
        console.print("  [cyan]minimal[/cyan]  Blank configuration to customize")
        console.print()
        console.print("Usage: [bold]osa local init <template>[/bold]")
        console.print()
        console.print("Example: [dim]osa local init geo[/dim]")
        sys.exit(0)

    # At this point template is guaranteed to be set (sys.exit never returns)
    assert template is not None

    # Check if already initialized
    if paths.is_initialized() and not force:
        console.warning("OSA is already initialized")
        console.print(f"  [dim]Config:[/dim] {paths.config_file}")
        console.info("Use --force to reinitialize")
        sys.exit(0)

    # Create all directories
    paths.ensure_directories()

    # Generate and write config
    config_content = _get_template_content(template, paths)
    paths.config_file.write_text(config_content)

    # Show results
    console.success(f"Initialized OSA with '{template}' template")
    console.print()
    console.print(f"  [cyan]Config:[/cyan]  {paths.config_file}")
    console.print(f"  [cyan]Data:[/cyan]    {paths.data_dir}/")
    console.print(f"  [cyan]State:[/cyan]   {paths.state_dir}/")
    console.print(f"  [cyan]Cache:[/cyan]   {paths.cache_dir}/")
    console.print()
    console.print(f"Edit your config: [bold]{paths.config_file}[/bold]")
    console.print("Then run: [bold]osa local start[/bold]")
