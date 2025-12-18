"""Search commands."""

import typer

app = typer.Typer(help="Search commands")


def get_server_url() -> str:
    """Get server URL from config or environment."""
    import os

    return os.environ.get("OSA_SERVER", "http://localhost:8000")


@app.command()
def vector(
    query: str = typer.Argument(..., help="Natural language search query"),
    limit: int = typer.Option(20, help="Maximum number of results"),
) -> None:
    """Search using vector similarity."""
    import httpx

    server_url = get_server_url()
    url = f"{server_url}/api/v1/search/vector"

    try:
        with httpx.Client() as client:
            response = client.get(url, params={"q": query, "limit": limit})
            response.raise_for_status()
            data = response.json()

            total = data.get("total", 0)
            results = data.get("results", [])

            typer.echo(f"Found {total} results:\n")

            for i, hit in enumerate(results, 1):
                score = hit.get("score", 0)
                metadata = hit.get("metadata", {})
                title = metadata.get("title", "Untitled")
                organism = metadata.get("organism", "Unknown")
                sample_count = metadata.get("sample_count", "?")
                pub_date = metadata.get("pub_date", "Unknown")

                typer.echo(f"{i}. {hit.get('id')} (score: {score:.2f})")
                typer.echo(f"   Title: {title}")
                typer.echo(f"   Organism: {organism} | Samples: {sample_count} | Published: {pub_date}")
                typer.echo()

    except httpx.ConnectError:
        typer.echo(f"Error: Could not connect to server at {server_url}", err=True)
        typer.echo("Is the server running? Start it with: osa server start", err=True)
        raise typer.Exit(1)
    except httpx.HTTPStatusError as e:
        typer.echo(f"Error: {e.response.status_code} - {e.response.text}", err=True)
        raise typer.Exit(1)


@app.command()
def keyword(
    query: str = typer.Argument(..., help="Keyword search query"),
    limit: int = typer.Option(20, help="Maximum number of results"),
) -> None:
    """Search using keyword matching (future)."""
    typer.echo("Keyword search is not yet implemented.", err=True)
    raise typer.Exit(1)
