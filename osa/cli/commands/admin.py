"""Administrative commands for OSA management."""

import shutil
import sys
from pathlib import Path

import cyclopts

from osa.infrastructure.local import DaemonManager, ServerStatus
from osa.infrastructure.local.paths import OSAPaths

app = cyclopts.App(name="admin", help="Administrative commands")


@app.command
def clean(
    force: bool = False,
    keep_logs: bool = False,
) -> None:
    """Wipe the ~/.osa directory to start fresh.

    Stops the server if running, then removes all data including:
    - Database (SQLite)
    - Vector indexes (ChromaDB)
    - Server state
    - Logs (unless --keep-logs)

    Args:
        force: Skip confirmation prompt.
        keep_logs: Keep the logs directory.
    """
    paths = OSAPaths()

    if not paths.base.exists():
        print("Nothing to clean - ~/.osa does not exist")
        return

    # Check if server is running
    daemon = DaemonManager()
    status_info = daemon.status()

    if status_info.status == ServerStatus.RUNNING:
        if not force:
            print(f"Server is running (PID {status_info.pid})")
            response = input("Stop server and clean? [y/N] ").strip().lower()
            if response != "y":
                print("Aborted")
                sys.exit(1)
        print("Stopping server...")
        daemon.stop()

    # Confirm before wiping
    if not force:
        print(f"This will delete: {paths.base}")
        if keep_logs:
            print(f"  (keeping: {paths.logs_dir})")
        response = input("Are you sure? [y/N] ").strip().lower()
        if response != "y":
            print("Aborted")
            sys.exit(1)

    # Perform cleanup
    if keep_logs:
        # Delete everything except logs
        for item in paths.base.iterdir():
            if item == paths.logs_dir:
                continue
            if item.is_dir():
                shutil.rmtree(item)
            else:
                item.unlink()
        print(f"Cleaned {paths.base} (logs preserved)")
    else:
        shutil.rmtree(paths.base)
        print(f"Removed {paths.base}")

    print("Run 'osa server start' to start fresh")


@app.command
def info() -> None:
    """Show information about the OSA data directory."""
    paths = OSAPaths()

    print(f"Base directory: {paths.base}")
    print()

    if not paths.base.exists():
        print("Directory does not exist")
        return

    # Show directory sizes
    def dir_size(path: Path) -> str:
        if not path.exists():
            return "does not exist"
        if path.is_file():
            size = path.stat().st_size
        else:
            size = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

        for unit in ["B", "KB", "MB", "GB"]:
            if size < 1024:
                return f"{size:.1f} {unit}"
            size /= 1024
        return f"{size:.1f} TB"

    print("Contents:")
    print(f"  Database:  {paths.base / 'osa.db'} ({dir_size(paths.base / 'osa.db')})")
    print(f"  Vectors:   {paths.vectors_dir} ({dir_size(paths.vectors_dir)})")
    print(f"  Logs:      {paths.logs_dir} ({dir_size(paths.logs_dir)})")

    # Show server state
    state = paths.read_server_state()
    if state:
        print()
        print(f"Server state: PID {state.pid}, started {state.started_at}")
