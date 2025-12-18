"""Server start/stop commands."""

import typer

from osa.infrastructure.local import DaemonManager, ServerStatus

app = typer.Typer(help="Server management commands")


@app.command()
def start(
    host: str = typer.Option("0.0.0.0", help="Host to bind to"),
    port: int = typer.Option(8000, help="Port to bind to"),
) -> None:
    """Start the OSA server in the background."""
    daemon = DaemonManager()

    try:
        info = daemon.start(host=host, port=port)
        typer.echo(f"Server started on http://{host}:{port} (PID {info.pid})")
        typer.echo(f"Logs: {daemon.paths.server_log}")
    except RuntimeError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def stop() -> None:
    """Stop the OSA server."""
    daemon = DaemonManager()

    try:
        daemon.stop()
        typer.echo("Server stopped")
    except RuntimeError as e:
        typer.echo(f"Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def status() -> None:
    """Check server status."""
    daemon = DaemonManager()
    info = daemon.status()

    if info.status == ServerStatus.RUNNING:
        typer.echo(f"Server is running on http://{info.host}:{info.port} (PID {info.pid})")
        if info.started_at:
            typer.echo(f"Started at: {info.started_at}")
    elif info.status == ServerStatus.STOPPED:
        typer.echo("Server is not running")
    elif info.status == ServerStatus.STALE:
        typer.echo(f"Server has stale state (PID {info.pid} is dead)")
        typer.echo("Run 'osa server stop' to clean up, or 'osa server start' to start fresh")
