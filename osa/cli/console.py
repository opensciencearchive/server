"""Console output for the CLI.

Provides a Console class that wraps rich for consistent, polished output.
All CLI output should go through this module.
"""

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from rich.console import Console as RichConsole
from rich.panel import Panel
from rich.table import Table

if TYPE_CHECKING:
    from osa.cli.models import SearchHit


def relative_time(iso_timestamp: str) -> str:
    """Convert ISO timestamp to relative time string (e.g., '2 hours ago')."""
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        now = datetime.now(timezone.utc)
        delta = now - dt

        seconds = delta.total_seconds()
        if seconds < 60:
            return "just now"
        elif seconds < 3600:
            mins = int(seconds // 60)
            return f"{mins} minute{'s' if mins != 1 else ''} ago"
        elif seconds < 86400:
            hours = int(seconds // 3600)
            return f"{hours} hour{'s' if hours != 1 else ''} ago"
        elif seconds < 604800:
            days = int(seconds // 86400)
            return f"{days} day{'s' if days != 1 else ''} ago"
        else:
            return dt.strftime("%Y-%m-%d %H:%M")
    except (ValueError, TypeError):
        return iso_timestamp


class Console:
    """CLI output manager wrapping rich.

    Provides consistent formatting for success/error messages, tables,
    and structured output. Respects TTY detection and can be configured
    for different verbosity levels.
    """

    def __init__(
        self,
        *,
        force_terminal: bool | None = None,
        quiet: bool = False,
    ) -> None:
        """Initialize the console.

        Args:
            force_terminal: Force terminal mode (True/False) or auto-detect (None).
            quiet: Suppress non-essential output.
        """
        self._console = RichConsole(
            force_terminal=force_terminal,
            stderr=False,
        )
        self._err_console = RichConsole(
            force_terminal=force_terminal,
            stderr=True,
        )
        self._quiet = quiet

    @property
    def is_terminal(self) -> bool:
        """Check if output is going to a terminal."""
        return self._console.is_terminal

    # -------------------------------------------------------------------------
    # Status messages
    # -------------------------------------------------------------------------

    def success(self, message: str) -> None:
        """Print a success message."""
        self._console.print(f"[green]\u2713[/green] {message}")

    def error(self, message: str, *, hint: str | None = None) -> None:
        """Print an error message to stderr."""
        self._err_console.print(f"[red]\u2717[/red] {message}")
        if hint:
            self._err_console.print(f"  [dim]{hint}[/dim]")

    def warning(self, message: str) -> None:
        """Print a warning message."""
        self._console.print(f"[yellow]\u26a0[/yellow] {message}")

    def info(self, message: str) -> None:
        """Print an info message (suppressed in quiet mode)."""
        if not self._quiet:
            self._console.print(f"[dim]{message}[/dim]")

    # -------------------------------------------------------------------------
    # Structured output
    # -------------------------------------------------------------------------

    def print(self, *args: Any, **kwargs: Any) -> None:
        """Print to console (pass-through to rich)."""
        self._console.print(*args, **kwargs)

    def print_lines(self, lines: list[str]) -> None:
        """Print multiple lines."""
        for line in lines:
            self._console.print(line)

    def table(
        self,
        rows: list[dict[str, Any]],
        columns: list[tuple[str, str]],  # (key, header)
        *,
        title: str | None = None,
        numbered: bool = False,
    ) -> None:
        """Print a table.

        Args:
            rows: List of dicts containing row data.
            columns: List of (key, header) tuples defining columns.
            title: Optional table title.
            numbered: Add a # column with row numbers.
        """
        table = Table(title=title, show_header=True, header_style="bold")

        if numbered:
            table.add_column("#", style="dim", width=3)

        for key, header in columns:
            table.add_column(header)

        for i, row in enumerate(rows, 1):
            values = [str(row.get(key, "")) for key, _ in columns]
            if numbered:
                table.add_row(str(i), *values)
            else:
                table.add_row(*values)

        self._console.print(table)

    def panel(
        self,
        content: str,
        *,
        title: str | None = None,
        subtitle: str | None = None,
        border_style: str = "dim",
    ) -> None:
        """Print content in a panel/box."""
        self._console.print(
            Panel(
                content,
                title=title,
                subtitle=subtitle,
                border_style=border_style,
            )
        )

    def record_detail(self, hit: "SearchHit") -> None:
        """Print detailed record view."""
        meta = hit.metadata
        lines = []

        if meta.summary:
            lines.append(meta.summary)
            lines.append("")

        # Metadata line
        meta_parts = []
        if meta.organism:
            meta_parts.append(f"[cyan]Organism:[/cyan] {meta.organism}")
        if meta.sample_count:
            meta_parts.append(f"[cyan]Samples:[/cyan] {meta.sample_count}")
        if meta.pub_date:
            meta_parts.append(f"[cyan]Published:[/cyan] {meta.pub_date}")

        if meta_parts:
            lines.append("    ".join(meta_parts))

        content = "\n".join(lines) if lines else "[dim]No details available[/dim]"

        self._console.print(
            Panel(
                content,
                title=f"[bold]{meta.title}[/bold]",
                subtitle=f"[dim]{hit.srn}[/dim]",
                border_style="blue",
                padding=(1, 2),
            )
        )

    # -------------------------------------------------------------------------
    # Search results
    # -------------------------------------------------------------------------

    def search_results(self, results: list["SearchHit"], total: int, index: str) -> None:
        """Print search results in a consistent format."""
        if not results:
            self.warning(f"No results found in index '{index}'")
            return

        self._console.print(f"Found {total} result{'s' if total != 1 else ''}:\n")

        for i, hit in enumerate(results, 1):
            meta = hit.metadata
            organism = meta.organism or "Unknown"
            samples = meta.sample_count or "?"
            pub_date = meta.pub_date or ""

            # Title line with number (no truncation, let it wrap)
            self._console.print(f"[bold blue][{i}][/bold blue] {meta.title}")

            # Metadata line: short_id · organism · samples · date
            meta_parts = [f"[dim]{hit.short_id}[/dim]", f"[dim]{organism}[/dim]"]
            if samples:
                meta_parts.append(f"[dim]{samples} samples[/dim]")
            if pub_date:
                meta_parts.append(f"[dim]{pub_date}[/dim]")

            self._console.print("    " + " · ".join(meta_parts))
            self._console.print()

    def search_hint(self) -> None:
        """Print hint about using osa show."""
        self.info("Use 'osa show <#>' to view details")

    # -------------------------------------------------------------------------
    # Progress and status
    # -------------------------------------------------------------------------

    def status(self, message: str):
        """Return a status context manager for long operations.

        Usage:
            with console.status("Loading..."):
                do_something()
        """
        return self._console.status(message)


# Module-level default instance for convenience
_default: Console | None = None


def get_console() -> Console:
    """Get the default console instance."""
    global _default
    if _default is None:
        _default = Console()
    return _default
