"""T032 — CsvGzipSerializer gzip-stream round-trip and magic bytes."""

import csv
import gzip
import io
from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest

from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.serializer.csv_gzip import CsvGzipSerializer
from osa.domain.semantics.model.value import FieldType

COLUMNS = [
    ColumnSpec(name="id", type=FieldType.TEXT),
    ColumnSpec(name="name", type=FieldType.TEXT),
]


async def _aiter(rows: list[Mapping[str, Any]]) -> AsyncIterator[Mapping[str, Any]]:
    for r in rows:
        yield r


async def _collect(gen: AsyncIterator[bytes]) -> bytes:
    out = b""
    async for chunk in gen:
        out += chunk
    return out


@pytest.mark.asyncio
async def test_gzip_magic_bytes() -> None:
    body = await _collect(CsvGzipSerializer().stream(_aiter([{"id": "a", "name": "x"}]), COLUMNS))
    # gzip stream header: 0x1f 0x8b 0x08
    assert body[:3] == b"\x1f\x8b\x08"


@pytest.mark.asyncio
async def test_gzip_roundtrip_through_gunzip() -> None:
    rows = [{"id": "a", "name": "alpha"}, {"id": "b", "name": "beta"}]
    body = await _collect(CsvGzipSerializer().stream(_aiter(rows), COLUMNS))
    text = gzip.decompress(body).decode()
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed[0] == ["id", "name"]
    assert parsed[1] == ["a", "alpha"]
    assert parsed[2] == ["b", "beta"]


@pytest.mark.asyncio
async def test_gzip_empty_result_roundtrips_to_header() -> None:
    body = await _collect(CsvGzipSerializer().stream(_aiter([]), COLUMNS))
    text = gzip.decompress(body).decode()
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed == [["id", "name"]]
