"""T031 — CsvSerializer header row, quoting, empty result."""

import csv
import io
from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest

from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.serializer.csv import CsvSerializer
from osa.domain.semantics.model.value import FieldType

COLUMNS = [
    ColumnSpec(name="id", type=FieldType.TEXT),
    ColumnSpec(name="name", type=FieldType.TEXT),
]


async def _aiter(rows: list[Mapping[str, Any]]) -> AsyncIterator[Mapping[str, Any]]:
    for r in rows:
        yield r


async def _collect(gen: AsyncIterator[bytes]) -> str:
    out = b""
    async for chunk in gen:
        out += chunk
    return out.decode()


@pytest.mark.asyncio
async def test_csv_header_and_rows() -> None:
    rows = [{"id": "a", "name": "alpha"}, {"id": "b", "name": "beta"}]
    text = await _collect(CsvSerializer().stream(_aiter(rows), COLUMNS))
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed[0] == ["id", "name"]
    assert parsed[1] == ["a", "alpha"]
    assert parsed[2] == ["b", "beta"]


@pytest.mark.asyncio
async def test_csv_empty_result_is_header_only() -> None:
    text = await _collect(CsvSerializer().stream(_aiter([]), COLUMNS))
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed == [["id", "name"]]


@pytest.mark.asyncio
async def test_csv_quotes_values_with_commas() -> None:
    rows = [{"id": "a", "name": "beta, gamma"}]
    text = await _collect(CsvSerializer().stream(_aiter(rows), COLUMNS))
    # QUOTE_MINIMAL wraps the comma-bearing value
    assert '"beta, gamma"' in text
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed[1] == ["a", "beta, gamma"]


@pytest.mark.asyncio
async def test_csv_missing_column_is_empty() -> None:
    rows = [{"id": "a"}]  # no "name"
    text = await _collect(CsvSerializer().stream(_aiter(rows), COLUMNS))
    parsed = list(csv.reader(io.StringIO(text)))
    assert parsed[1] == ["a", ""]
