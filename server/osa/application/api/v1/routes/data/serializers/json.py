"""JSON serializer — paginated envelope
``{"rows": [...], "next_cursor": ..., "has_more": ...}``.

Built row-by-row so the whole page is never held twice in memory. The
``next_cursor`` and ``has_more`` are precomputed by the query service and passed
in; an empty result yields ``{"rows": [], "next_cursor": null, "has_more": false}``
with HTTP 200. ``has_more`` lets a consumer distinguish a *complete* page from a
*truncated* one without inspecting the cursor.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, ClassVar

from osa.domain.data.model.manifest import ColumnSpec


class JsonSerializer:
    media_type: ClassVar[str] = "application/json"

    async def stream(
        self,
        rows: AsyncIterator[Mapping[str, Any]],
        columns: Sequence[ColumnSpec],
        *,
        next_cursor: str | None = None,
        has_more: bool = False,
    ) -> AsyncIterator[bytes]:
        yield b'{"rows":['
        first = True
        async for row in rows:
            projected = {col.name: row.get(col.name) for col in columns}
            chunk = json.dumps(projected, default=str).encode()
            if first:
                first = False
            else:
                chunk = b"," + chunk
            yield chunk
        cursor_json = json.dumps(next_cursor).encode()
        has_more_json = b"true" if has_more else b"false"
        yield b'],"next_cursor":' + cursor_json + b',"has_more":' + has_more_json + b"}"
