"""JSON serializer — paginated envelope ``{"rows": [...], "next_cursor": ...}``.

Built row-by-row so the whole page is never held twice in memory. The
``next_cursor`` is precomputed by the query service and passed in; an empty
result yields ``{"rows": [], "next_cursor": null}`` with HTTP 200.
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
        yield b'],"next_cursor":' + cursor_json + b"}"
