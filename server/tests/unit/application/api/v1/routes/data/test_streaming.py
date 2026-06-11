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
from osa.application.api.v1.routes.data.formats import FORMATS
from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.model.query_plan import (
    QueryPlan,
    SortDirection,
    SortSpec,
    TableKind,
)
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


@pytest.mark.asyncio
async def test_paginated_records_id_sort_encodes_srn() -> None:
    # ``sort=id`` on records aliases to the srn column in the store, so the
    # cursor's sort value must be the srn too. Encoding the bare id would
    # compare it against the srn column — every SRN sorts after a bare id, so
    # the keyset matches all rows and pagination never advances.
    rows = [
        {"id": "a", "srn": "urn:osa:localhost:rec:a@1"},
        {"id": "b", "srn": "urn:osa:localhost:rec:b@1"},
        {"id": "c", "srn": "urn:osa:localhost:rec:c@1"},
    ]
    plan = QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.RECORDS,
        pagination={"limit": 2},
        sort=[SortSpec(column="id", direction=SortDirection.ASC)],
    )
    resp = await build_table_response(_aiter(rows), JSON_FMT, COLUMNS, plan)
    parsed = json.loads(await _body(resp))
    decoded = json.loads(base64.urlsafe_b64decode(parsed["next_cursor"]))
    assert decoded["s"] == "urn:osa:localhost:rec:b@1"
    assert decoded["id"] == "urn:osa:localhost:rec:b@1"


@pytest.mark.asyncio
async def test_paginated_features_id_sort_encodes_row_id() -> None:
    # Feature rows have a real integer id column (no srn key), so ``sort=id``
    # keeps encoding the row's own id.
    rows = [
        {"id": 1, "record_srn": "urn:osa:localhost:rec:a@1"},
        {"id": 2, "record_srn": "urn:osa:localhost:rec:a@1"},
        {"id": 3, "record_srn": "urn:osa:localhost:rec:a@1"},
    ]
    plan = QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.FEATURE,
        feature_name="chem_features",
        pagination={"limit": 2},
        # default FEATURE sort is id asc
    )
    resp = await build_table_response(_aiter(rows), JSON_FMT, COLUMNS, plan)
    parsed = json.loads(await _body(resp))
    decoded = json.loads(base64.urlsafe_b64decode(parsed["next_cursor"]))
    assert decoded["s"] == 2
    assert decoded["id"] == 2


@pytest.mark.asyncio
async def test_paginated_feature_hook_column_named_srn_does_not_hijack_tiebreak() -> None:
    # A hook may legally declare a data column named "srn" (it's not a feature
    # auto column). The cursor tiebreaker must still be the integer row id —
    # picking the hook column's string would 400 on the next page when it's
    # coerced against the BIGINT id column.
    rows = [
        {"id": 1, "record_srn": "urn:osa:localhost:rec:a@1", "srn": "hook-value-1"},
        {"id": 2, "record_srn": "urn:osa:localhost:rec:a@1", "srn": "hook-value-2"},
        {"id": 3, "record_srn": "urn:osa:localhost:rec:a@1", "srn": "hook-value-3"},
    ]
    plan = QueryPlan(
        schema_id=SCHEMA,
        table_kind=TableKind.FEATURE,
        feature_name="chem_features",
        pagination={"limit": 2},
        # default FEATURE sort is id asc
    )
    resp = await build_table_response(_aiter(rows), JSON_FMT, COLUMNS, plan)
    parsed = json.loads(await _body(resp))
    decoded = json.loads(base64.urlsafe_b64decode(parsed["next_cursor"]))
    assert decoded["s"] == 2
    assert decoded["id"] == 2
