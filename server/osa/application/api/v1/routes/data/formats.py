"""Response-format registry for the ``/data/`` surface.

Each :class:`DataResponseFormat` ties a URL suffix to its serializer,
pagination semantics, and per-route statement-timeout budget (research §7).
Adding a format (e.g. ``ndjson``, ``parquet``) is a one-line append plus one
serializer class — the route factory picks it up automatically.

This is presentation configuration, so it lives with the routes: media types
and Postgres execution budgets are HTTP/adapter concerns, not domain model.
The domain's :class:`~osa.domain.data.model.query_plan.QueryPlan` stays a pure
query description.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from osa.application.api.v1.routes.data.serializers.csv import CsvSerializer
from osa.application.api.v1.routes.data.serializers.csv_gzip import CsvGzipSerializer
from osa.application.api.v1.routes.data.serializers.json import JsonSerializer
from osa.application.api.v1.routes.data.serializers.protocol import Serializer


@dataclass(frozen=True)
class DataResponseFormat:
    serializer_cls: type[Serializer]
    paginated: bool  # True → JSON envelope + cursor; False → unbounded stream
    suffix: str  # "" (json), "csv", "csv.gz"
    timeout: timedelta  # statement budget: short for paginated, long for dumps

    @property
    def media_type(self) -> str:
        # The serializer is the single owner of its media type.
        return self.serializer_cls.media_type

    def make_serializer(self) -> Serializer:
        return self.serializer_cls()


FORMATS: tuple[DataResponseFormat, ...] = (
    DataResponseFormat(
        serializer_cls=JsonSerializer,
        paginated=True,
        suffix="",
        timeout=timedelta(seconds=30),
    ),
    DataResponseFormat(
        serializer_cls=CsvSerializer,
        paginated=False,
        suffix="csv",
        timeout=timedelta(minutes=30),
    ),
    DataResponseFormat(
        serializer_cls=CsvGzipSerializer,
        paginated=False,
        suffix="csv.gz",
        timeout=timedelta(minutes=30),
    ),
)
