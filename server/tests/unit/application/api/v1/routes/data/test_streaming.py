"""Unit tests for build_table_response — pre-flight, streaming, pagination, cursor."""

import base64
import csv
import gzip
import io
import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest

from osa.application.api.v1.routes.data._streaming import build_table_response
from osa.domain.data.model.format import FORMATS
from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.model.query_plan import QueryPlan, TableKind
from osa.domain.semantics.model.value import FieldType
from osa.domain.shared.model.srn import SchemaId

SCHEMA = SchemaId.parse("compound@1.0.0")
COLUMNS = [ColumnSpec(name="id", type=FieldType.TEXT), ColumnSpec(name="srn", type=FieldType.TEXT)]
JSON_FMT = next(f for f in FORMATS if f.suffix == "")
CSV_FMT = next(f for f in FORMATS if f.suffix == "csv")
GZ_FMT = next(f for f in FORMATS if f.suffix == "csv.gz")


async def _aiter(rows: list[Mapping[str, Any]]) -> AsyncIterator[Mapping[str, Any]]:
    for r in rows:
        yield r


async def _raising() -> AsyncIterator[Mapping[str, Any]]:
    raise ValueError("boom before first row")
    yield  # pragma: no cover


async def _body(resp) -> bytes:
    out = b""
    async for chunk in resp.body_iterator:
        out += chunk.encode() if isinstance(chunk, str) else chunk
    return out


def _plan(limit: int = 50) -> QueryPlan:
    return QueryPlan(schema_id=SCHEMA, table_kind=TableKind.RECORDS, pagination={"limit": limit})


@pytest.mark.asyncio
async def test_streaming_preflight_raises_before_bytes() -> None:
    # An error on the first __anext__ must propagate (→ 4xx), not corrupt a stream.
    with pytest.raises(ValueError, match="boom"):
        await build_table_response(_raising(), CSV_FMT, COLUMNS, _plan())


@pytest.mark.asyncio
async def test_streaming_csv_full_table() -> None:
    rows = [{"id": "a", "srn": "s1"}, {"id": "b", "srn": "s2"}]
    resp = await build_table_response(_aiter(rows), CSV_FMT, COLUMNS, _plan())
    parsed = list(csv.reader(io.StringIO((await _body(resp)).decode())))
    assert parsed[0] == ["id", "srn"]
    assert parsed[1:] == [["a", "s1"], ["b", "s2"]]


@pytest.mark.asyncio
async def test_streaming_gzip_roundtrip() -> None:
    rows = [{"id": "a", "srn": "s1"}]
    resp = await build_table_response(_aiter(rows), GZ_FMT, COLUMNS, _plan())
    text = gzip.decompress(await _body(resp)).decode()
    assert "id,srn" in text and "a,s1" in text


@pytest.mark.asyncio
async def test_streaming_empty_is_header_only() -> None:
    resp = await build_table_response(_aiter([]), CSV_FMT, COLUMNS, _plan())
    parsed = list(csv.reader(io.StringIO((await _body(resp)).decode())))
    assert parsed == [["id", "srn"]]


@pytest.mark.asyncio
async def test_paginated_no_more_rows_null_cursor() -> None:
    rows = [{"id": "a", "srn": "s1"}, {"id": "b", "srn": "s2"}]
    resp = await build_table_response(_aiter(rows), JSON_FMT, COLUMNS, _plan(limit=50))
    parsed = json.loads(await _body(resp))
    assert parsed["next_cursor"] is None
    assert parsed["rows"] == [{"id": "a", "srn": "s1"}, {"id": "b", "srn": "s2"}]


@pytest.mark.asyncio
async def test_paginated_has_more_emits_cursor() -> None:
    # limit=2, but 3 rows available → has_more, cursor derived from 2nd row's srn.
    rows = [
        {"id": "a", "srn": "s1", "created_at": "2026-01-03"},
        {"id": "b", "srn": "s2", "created_at": "2026-01-02"},
        {"id": "c", "srn": "s3", "created_at": "2026-01-01"},
    ]
    resp = await build_table_response(_aiter(rows), JSON_FMT, COLUMNS, _plan(limit=2))
    parsed = json.loads(await _body(resp))
    assert len(parsed["rows"]) == 2
    assert parsed["next_cursor"] is not None
    decoded = json.loads(base64.urlsafe_b64decode(parsed["next_cursor"]))
    # default RECORDS sort is created_at desc, tiebreaker srn
    assert decoded["s"] == "2026-01-02"
    assert decoded["id"] == "s2"
