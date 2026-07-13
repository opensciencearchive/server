"""``ui://osa/*`` widget resource provider (#162).

Serves the compiled widget bundles from ``packages/osa-widgets/`` — one
self-contained HTML file per widget, built in CI and shipped with the server
image (never fetched from an external origin at runtime). The bundle
directory comes from ``Config.mcp.widget_bundle_dir``; ``None`` means the
packaged default (``osa/application/api/mcp/bundles/``).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from mcp.server.lowlevel.helper_types import ReadResourceContents

from osa.application.api.mcp.meta import MCP_APP_MIME, ResourceMeta
from osa.domain.shared.error import NotFoundError

_DEFAULT_BUNDLE_DIR = Path(__file__).parent / "bundles"


@dataclass(frozen=True)
class WidgetDef:
    """One baseline widget: its resource URI and bundle filename."""

    uri: str
    title: str
    description: str
    filename: str


WIDGETS: tuple[WidgetDef, ...] = (
    WidgetDef(
        uri="ui://osa/dataset-overview",
        title="Dataset overview",
        description="Cards for every published dataset with row counts.",
        filename="dataset-overview.html",
    ),
    WidgetDef(
        uri="ui://osa/table",
        title="Data table",
        description="Sortable, server-paginated table over any records or feature table.",
        filename="table.html",
    ),
    WidgetDef(
        uri="ui://osa/chart",
        title="Chart",
        description="Interactive line/scatter/bar chart with client-side binning.",
        filename="chart.html",
    ),
    WidgetDef(
        uri="ui://osa/record",
        title="Record detail",
        description="Single-record card with joined feature rows and run provenance.",
        filename="record.html",
    ),
    WidgetDef(
        uri="ui://osa/filter-panel",
        title="Filter panel",
        description="Faceted filter controls derived from the schema manifest.",
        filename="filter-panel.html",
    ),
)


class WidgetRegistry:
    """Resolves ``ui://osa/*`` URIs to compiled bundle files on disk."""

    def __init__(self, bundle_dir: Path | None = None) -> None:
        self._bundle_dir = bundle_dir or _DEFAULT_BUNDLE_DIR
        self._by_uri = {w.uri: w for w in WIDGETS}

    def read(self, uri: str) -> ReadResourceContents:
        """Read one widget bundle. Raises :class:`NotFoundError` for unknown
        URIs and for registered widgets whose bundle was never built."""
        widget = self._by_uri.get(uri)
        if widget is None:
            raise NotFoundError(f"Unknown UI resource {uri!r}", code="resource_not_found")
        path = self._bundle_dir / widget.filename
        if not path.is_file():
            raise NotFoundError(
                f"Widget bundle {widget.filename!r} is not present in {self._bundle_dir} — "
                "build it with `just widgets-build` (or ship an image that bakes the bundles).",
                code="bundle_missing",
            )
        return ReadResourceContents(
            content=path.read_text(encoding="utf-8"),
            mime_type=MCP_APP_MIME,
            meta=ResourceMeta().dump(),
        )
