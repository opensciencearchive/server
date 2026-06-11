"""Gzip-while-streaming CSV serializer (research §1).

Wraps :class:`CsvSerializer` through ``zlib.compressobj(level=6,
wbits=MAX_WBITS|16)``. The ``MAX_WBITS|16`` flag selects gzip-stream mode
(proper gzip header + trailer) so the byte stream is exactly what ``gunzip``
and HTTP gzip clients expect. Memory footprint is the 32KB DEFLATE window plus
one CSV row, regardless of result size — the basis of the SC-001 bounded-memory
target.
"""

from __future__ import annotations

import zlib
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, ClassVar

from osa.domain.data.model.manifest import ColumnSpec
from osa.application.api.v1.routes.data.serializers.csv import CsvSerializer


class CsvGzipSerializer:
    media_type: ClassVar[str] = "application/gzip"

    def __init__(self) -> None:
        self._csv = CsvSerializer()

    async def stream(
        self,
        rows: AsyncIterator[Mapping[str, Any]],
        columns: Sequence[ColumnSpec],
        *,
        next_cursor: str | None = None,
    ) -> AsyncIterator[bytes]:
        compressor = zlib.compressobj(level=6, wbits=zlib.MAX_WBITS | 16)
        async for chunk in self._csv.stream(rows, columns):
            compressed = compressor.compress(chunk)
            if compressed:
                yield compressed
        tail = compressor.flush(zlib.Z_FINISH)
        if tail:
            yield tail
