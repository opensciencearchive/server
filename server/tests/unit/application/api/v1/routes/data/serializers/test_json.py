"""T030 — JsonSerializer paginated envelope, empty result, cursor embedding."""

import json
from collections.abc import AsyncIterator, Mapping
from typing import Any

import pytest

from osa.domain.data.model.manifest import ColumnSpec
from osa.application.api.v1.routes.data.serializers.json import JsonSerializer
from osa.domain.semantics.model.value import FieldType

COLUMNS = [
    ColumnSpec(name="id", type=FieldType.TEXT),
    ColumnSpec(name="mw", type=FieldType.NUMBER),
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
async def test_json_envelope_with_rows_and_cursor() -> None:
    rows = [{"id": "a", "mw": 1.5}, {"id": "b", "mw": 2.0}]
    body = await _collect(
        JsonSerializer().stream(_aiter(rows), COLUMNS, next_cursor="CURSOR", has_more=True)
    )
    parsed = json.loads(body)
    assert parsed["next_cursor"] == "CURSOR"
    assert parsed["has_more"] is True
    assert parsed["rows"] == [{"id": "a", "mw": 1.5}, {"id": "b", "mw": 2.0}]


@pytest.mark.asyncio
async def test_json_empty_result_is_valid() -> None:
    body = await _collect(JsonSerializer().stream(_aiter([]), COLUMNS, next_cursor=None))
    parsed = json.loads(body)
    assert parsed == {"rows": [], "next_cursor": None, "has_more": False}


@pytest.mark.asyncio
async def test_json_has_more_flag_defaults_false() -> None:
    # has_more lets an agent tell a complete page from a truncated one — the
    # ambiguity that sent the pilot session on a long detour. Default: no more.
    body = await _collect(JsonSerializer().stream(_aiter([{"id": "a", "mw": 1.5}]), COLUMNS))
    parsed = json.loads(body)
    assert parsed["has_more"] is False


@pytest.mark.asyncio
async def test_json_projects_only_declared_columns() -> None:
    rows = [{"id": "a", "mw": 1.5, "secret": "x"}]
    body = await _collect(JsonSerializer().stream(_aiter(rows), COLUMNS))
    parsed = json.loads(body)
    assert parsed["rows"] == [{"id": "a", "mw": 1.5}]
    assert parsed["next_cursor"] is None
