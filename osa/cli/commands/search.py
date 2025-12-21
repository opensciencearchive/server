"""Search commands."""

import os
import sys

import cyclopts
import httpx

app = cyclopts.App(name="search", help="Search commands")


def get_server_url() -> str:
    """Get server URL from config or environment."""
    return os.environ.get("OSA_SERVER", "http://localhost:8000")


@app.default
def search(
    index: str,
    query: str,
    /,
    limit: int = 20,
) -> None:
    """Search an index by name.

    Args:
        index: Name of the index to search (e.g., 'geo', 'vector')
        query: Search query
        limit: Maximum number of results
    """
    server_url = get_server_url()
    url = f"{server_url}/api/v1/search/{index}"

    try:
        with httpx.Client() as client:
            response = client.get(url, params={"q": query, "limit": limit})
            response.raise_for_status()
            data = response.json()

            total = data.get("total", 0)
            results = data.get("results", [])

            print(f"Found {total} results in index '{index}':\n")

            for i, hit in enumerate(results, 1):
                score = hit.get("score", 0)
                metadata = hit.get("metadata", {})
                title = metadata.get("title", "Untitled")
                organism = metadata.get("organism", "Unknown")
                sample_count = metadata.get("sample_count", "?")
                pub_date = metadata.get("pub_date", "Unknown")

                print(f"{i}. {hit.get('id')} (score: {score:.2f})")
                print(f"   Title: {title}")
                print(f"   Organism: {organism} | Samples: {sample_count} | Published: {pub_date}")
                print()

    except httpx.ConnectError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        print("Is the server running? Start it with: osa server start", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(e)
        print(f"Error: {e.response.status_code} - {e.response.text}", file=sys.stderr)
        sys.exit(1)


@app.command
def list_indexes() -> None:
    """List available search indexes."""
    server_url = get_server_url()
    url = f"{server_url}/api/v1/search/"

    try:
        with httpx.Client() as client:
            response = client.get(url)
            response.raise_for_status()
            data = response.json()

            indexes = data.get("indexes", [])
            if indexes:
                print("Available indexes:")
                for idx in indexes:
                    print(f"  - {idx}")
            else:
                print("No indexes configured")

    except httpx.ConnectError:
        print(f"Error: Could not connect to server at {server_url}", file=sys.stderr)
        sys.exit(1)
    except httpx.HTTPStatusError as e:
        print(f"Error: {e.response.status_code} - {e.response.text}", file=sys.stderr)
        sys.exit(1)
