"""Administrative commands for OSA management."""

import shutil
import sys
from pathlib import Path

import cyclopts

from osa.cli.console import get_console
from osa.cli.util import DaemonManager, OSAPaths, ServerStatus, read_server_state

app = cyclopts.App(name="admin", help="Administrative commands")


@app.command
def clean(
    force: bool = False,
    keep_config: bool = False,
    keep_logs: bool = False,
) -> None:
    """Wipe OSA data directories to start fresh.

    Stops the server if running, then removes:
    - ~/.local/share/osa/ (database, vectors)
    - ~/.local/state/osa/ (server state, logs)
    - ~/.cache/osa/ (search cache)
    - ~/.config/osa/ (unless --keep-config)

    Args:
        force: Skip confirmation prompt.
        keep_config: Keep the config directory (~/.config/osa).
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
    console.info("Run 'osa init' to set up OSA again")


@app.command
def info() -> None:
    """Show information about OSA directories and status."""
    console = get_console()
    paths = OSAPaths()

    def dir_size(path: Path) -> str:
        """Get human-readable size of a file or directory."""
        if not path.exists():
            return "—"
        if path.is_file():
            size = path.stat().st_size
        else:
            size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    def exists_marker(path: Path) -> str:
        return "[green]✓[/green]" if path.exists() else "[dim]✗[/dim]"

    console.print("[bold]OSA Directories[/bold]\n")

    # Config
    console.print(f"{exists_marker(paths.config_dir)} [cyan]Config:[/cyan]  {paths.config_dir}")
    if paths.config_file.exists():
        console.print(f"    config.yaml  [dim]({dir_size(paths.config_file)})[/dim]")

    # Data
    console.print(f"{exists_marker(paths.data_dir)} [cyan]Data:[/cyan]    {paths.data_dir}")
    if paths.data_dir.exists():
        console.print(f"    osa.db       [dim]({dir_size(paths.database_file)})[/dim]")
        console.print(f"    vectors/     [dim]({dir_size(paths.vectors_dir)})[/dim]")

    # State
    console.print(f"{exists_marker(paths.state_dir)} [cyan]State:[/cyan]   {paths.state_dir}")
    if paths.state_dir.exists():
        console.print(f"    logs/        [dim]({dir_size(paths.logs_dir)})[/dim]")

    # Cache
    console.print(f"{exists_marker(paths.cache_dir)} [cyan]Cache:[/cyan]   {paths.cache_dir}")
    if paths.cache_dir.exists():
        console.print(f"    [dim]({dir_size(paths.cache_dir)})[/dim]")

    # Server status
    console.print()
    state = read_server_state(paths.server_state_file)
    if state:
        from osa.cli.console import relative_time

        console.print("[bold]Server[/bold]")
        console.print(f"  PID: {state.pid}")
        console.print(f"  Address: http://{state.host}:{state.port}")
        console.print(f"  Started: {relative_time(state.started_at)}")
    else:
        console.print("[dim]Server not running[/dim]")
