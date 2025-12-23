"""Show command for viewing record details."""

import sys

import cyclopts

from osa.cli.util import OSAPaths

app = cyclopts.App(name="show", help="Show record details from last search")


@app.default
def show(ref: str, /) -> None:
    """Show details for a record from the last search.

    Args:
        ref: Result number (1, 2, ...) or short ID (e.g., 'ede67f')
    """
    paths = OSAPaths()
    cache = paths.read_search_cache()

    if cache is None:
        print("No search results cached.", file=sys.stderr)
        print("Run a search first: osa search vector \"your query\"", file=sys.stderr)
        sys.exit(1)

    # Try to interpret ref as a number first
    result = None
    if ref.isdigit():
        idx = int(ref) - 1  # Convert to 0-based index
        if 0 <= idx < len(cache.results):
            result = cache.results[idx]
        else:
            print(
                f"Invalid result number: {ref} "
                f"(last search had {len(cache.results)} results)",
                file=sys.stderr,
            )
            sys.exit(1)
    else:
        # Try to match by short ID prefix
        ref_lower = ref.lower()
        matches = [r for r in cache.results if r["short_id"].startswith(ref_lower)]
        if len(matches) == 1:
            result = matches[0]
        elif len(matches) > 1:
            print(f"Ambiguous short ID '{ref}'. Matches:", file=sys.stderr)
            for m in matches:
                print(f"  {m['short_id']}  {m['metadata'].get('title', 'Untitled')}")
            sys.exit(1)
        else:
            print(f"No match for '{ref}' in last search results.", file=sys.stderr)
            sys.exit(1)

    # Display the result
    _print_record(result, cache.query, cache.index)


def _print_record(result: dict, query: str, index: str) -> None:
    """Print record details."""
    metadata = result.get("metadata", {})

    print(f"SRN: {result['srn']}")
    print(f"Short ID: {result['short_id']}")
    print()

    # Core fields
    if title := metadata.get("title"):
        print(f"Title: {title}")
    if summary := metadata.get("summary"):
        print(f"\nSummary:\n  {summary}")

    print()

    # Metadata fields
    fields = [
        ("Organism", "organism"),
        ("Samples", "sample_count"),
        ("Type", "type"),
        ("Published", "pub_date"),
        ("Platform", "platform"),
    ]

    for label, key in fields:
        if value := metadata.get(key):
            print(f"{label}: {value}")

    # Any other metadata
    shown_keys = {"title", "summary", "organism", "sample_count", "type", "pub_date", "platform"}
    other = {k: v for k, v in metadata.items() if k not in shown_keys and v}
    if other:
        print("\nOther metadata:")
        for k, v in other.items():
            print(f"  {k}: {v}")

    print()
    print(f"(from search: '{query}' in {index} index)")
