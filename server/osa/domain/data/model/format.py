"""Response-format registry for the ``/data/`` surface.

Each :class:`DataResponseFormat` ties a URL suffix to its serializer, media
type, pagination semantics, and per-route statement timeout (research §7).
Adding a format (e.g. ``ndjson``, ``parquet``) is a one-line append plus one
serializer class — the route factory and timeout dependency pick it up
automatically.
"""

from __future__ import annotations

from dataclasses import dataclass

from osa.domain.data.serializer.csv import CsvSerializer
from osa.domain.data.serializer.csv_gzip import CsvGzipSerializer
from osa.domain.data.serializer.json import JsonSerializer
from osa.domain.data.serializer.protocol import Serializer


@dataclass(frozen=True)
class DataResponseFormat:
    serializer_cls: type[Serializer]
    paginated: bool  # True → JSON envelope + cursor; False → unbounded stream
    suffix: str  # "" (json), "csv", "csv.gz"
    media_type: str
    statement_timeout: str  # "30s" paginated, "30min" streaming

    def make_serializer(self) -> Serializer:
        return self.serializer_cls()


FORMATS: tuple[DataResponseFormat, ...] = (
    DataResponseFormat(
        serializer_cls=JsonSerializer,
        paginated=True,
        suffix="",
        media_type="application/json",
        statement_timeout="30s",
    ),
    DataResponseFormat(
        serializer_cls=CsvSerializer,
        paginated=False,
        suffix="csv",
        media_type="text/csv",
        statement_timeout="30min",
    ),
    DataResponseFormat(
        serializer_cls=CsvGzipSerializer,
        paginated=False,
        suffix="csv.gz",
        media_type="application/gzip",
        statement_timeout="30min",
    ),
)
