"""CSV serializer — header row from columns, then one row per record.

Streams incrementally via a reusable :class:`_RowEncoder` that holds a single
``io.StringIO`` + ``csv.writer``, so only one row is ever materialised at a
time. An empty result still yields the header row followed by EOF. Quoting uses
``csv.QUOTE_MINIMAL``.
"""

from __future__ import annotations

import csv
import io
from collections.abc import AsyncIterator, Mapping, Sequence
from typing import Any, ClassVar


from osa.domain.data.model.manifest import ColumnSpec


class _RowEncoder:
    """Encodes one CSV row at a time, reusing a single buffer + writer.

    The writer's type is inferred from the ``csv.writer(...)`` assignment, so
    no untyped parameter is threaded through the serializer.
    """

    def __init__(self) -> None:
        self._buf = io.StringIO()
        self._writer = csv.writer(self._buf, quoting=csv.QUOTE_MINIMAL)

    def encode(self, values: Sequence[Any]) -> bytes:
        self._buf.seek(0)
        self._buf.truncate(0)
        self._writer.writerow(values)
        return self._buf.getvalue().encode()


class CsvSerializer:
    media_type: ClassVar[str] = "text/csv"

    async def stream(
        self,
        rows: AsyncIterator[Mapping[str, Any]],
        columns: Sequence[ColumnSpec],
        *,
        next_cursor: str | None = None,
    ) -> AsyncIterator[bytes]:
        names = [col.name for col in columns]
        encoder = _RowEncoder()

        yield encoder.encode(names)

        async for row in rows:
            yield encoder.encode([_stringify(row.get(name)) for name in names])


def _stringify(value: Any) -> Any:
    """Render non-scalar values deterministically; let csv handle scalars/None."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)
