"""Administrative commands for OSA management."""

from pathlib import Path

import cyclopts

from osa.cli.console import get_console
from osa.cli.util import OSAPaths, read_server_state

app = cyclopts.App(name="admin", help="Administrative commands")


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
