"""Response assembly for table reads — streaming and paginated paths.

Two shapes share one entry point :func:`build_table_response`:

* **Streaming** (``.csv`` / ``.csv.gz``): pre-flight pulls the first row inside
  a try (research §4). A parse/validation/planner error raised before the first
  row propagates to the route → mapped to a 4xx/404 *before any bytes*. On
  success the first row is chained back in and the whole iterator is streamed.

* **Paginated** (JSON): consume up to ``limit + 1`` rows; if a ``limit+1``-th
  row exists there's a next page, so derive ``next_cursor`` from the last
  returned row's ``(sort_value, srn)`` pair. The bounded page (``limit`` ≤ 1000)
  is then rendered by the JSON serializer.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any

from fastapi.responses import StreamingResponse

from osa.application.api.v1.routes.data.formats import DataResponseFormat
from osa.domain.data.model.manifest import ColumnSpec
from osa.domain.data.model.query_plan import QueryPlan


async def build_table_response(
    rows: AsyncIterator[Mapping[str, Any]],
    fmt: DataResponseFormat,
    columns: Sequence[ColumnSpec],
    plan: QueryPlan,
) -> StreamingResponse:
    if fmt.paginated:
        return await _paginated_response(rows, fmt, columns, plan)
    return await _streaming_response(rows, fmt, columns)


async def _streaming_response(
    rows: AsyncIterator[Mapping[str, Any]],
    fmt: DataResponseFormat,
    columns: Sequence[ColumnSpec],
) -> StreamingResponse:
    iterator = rows.__aiter__()
    # Pre-flight: surface setup/validation errors before the first byte.
    try:
        first = await iterator.__anext__()
        empty = False
    except StopAsyncIteration:
        empty = True

    async def chained() -> AsyncIterator[Mapping[str, Any]]:
        if not empty:
            yield first
        async for row in iterator:
            yield row

    serializer = fmt.make_serializer()
    return StreamingResponse(
        serializer.stream(chained(), columns),
        media_type=fmt.media_type,
    )


async def _paginated_response(
    rows: AsyncIterator[Mapping[str, Any]],
    fmt: DataResponseFormat,
    columns: Sequence[ColumnSpec],
    plan: QueryPlan,
) -> StreamingResponse:
    # plan.take_page owns the limit+1 mechanic and the next_cursor encoding
    # (shared with the view queries), so the encode side cannot drift from
    # the store's decode side.
    slice_ = await plan.take_page(rows)

    async def page_iter() -> AsyncIterator[Mapping[str, Any]]:
        for row in slice_.rows:
            yield row

    serializer = fmt.make_serializer()
    return StreamingResponse(
        serializer.stream(
            page_iter(), columns, next_cursor=slice_.next_cursor, has_more=slice_.truncated
        ),
        media_type=fmt.media_type,
    )
