"""SourceContext — manages file downloads and session state during source execution."""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import httpx

from osa.types.source import SourceFileRef


class SourceContext:
    """Context object provided to source.pull() for file downloads and session management.

    The SDK owns file placement — developers call add_file() with a URL and the context
    downloads the file to the correct location under $OSA_FILES.
    """

    def __init__(self, files_dir: Path, output_dir: Path) -> None:
        self._files_dir = files_dir
        self._output_dir = output_dir
        self._session: dict[str, Any] | None = None
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        import httpx as _httpx

        if self._client is None:
            self._client = _httpx.AsyncClient()
        return self._client

    async def add_file(self, source_id: str, name: str, *, url: str) -> SourceFileRef:
        """Download a file from url to $OSA_FILES/{source_id}/{name}.

        Returns a SourceFileRef for inclusion in the SourceRecord.
        """
        target_dir = self._files_dir / source_id
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / name

        client = await self._get_client()
        resp = await client.get(url)
        resp.raise_for_status()
        target.write_bytes(resp.content)

        relative_path = f"{source_id}/{name}"
        return SourceFileRef(name=name, relative_path=relative_path)

    def set_session(self, state: dict[str, Any]) -> None:
        """Set continuation state — written to $OSA_OUT/session.json on exit."""
        self._session = state

    def write_session(self) -> None:
        """Write session.json if session state was set. Called by the entrypoint."""
        if self._session is not None:
            session_path = self._output_dir / "session.json"
            session_path.write_text(json.dumps(self._session))

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
