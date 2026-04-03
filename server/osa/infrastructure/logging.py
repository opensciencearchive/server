"""OSA logging — custom logfire console exporter and structured logger.

Provides:
- ``OSAConsoleExporter``: logfire console formatter with aligned columns
  (timestamp, level, module, message) and indented continuation lines.
- ``get_logger(name)``: thin wrapper around logfire that auto-tags with module name.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, cast

import logfire as _logfire
from logfire._internal.exporters.console import (
    ATTRIBUTES_TAGS_KEY,
    ONE_SECOND_IN_NANOSECONDS,
    SimpleConsoleSpanExporter,
    _ERROR_LEVEL,
    _WARN_LEVEL,
)

if TYPE_CHECKING:
    from logfire._internal.exporters.console import Record, TextParts


_LEVEL_NAMES: dict[int, str] = {
    0: "TRACE",
    1: "DEBUG",
    5: "DEBUG",
    9: "INFO ",
    13: "WARN ",
    17: "ERROR",
    21: "FATAL",
}

# Module column width (inside brackets)
_MODULE_WIDTH = 20
# Fixed prefix: HH:MM:SS.mmm(12) + space(1) + LEVEL(5) + space(1) + [module](22) + space(1) = 42
_PREFIX_WIDTH = 42


class OSAConsoleExporter(SimpleConsoleSpanExporter):
    """Logfire console exporter with aligned columns.

    Format: ``HH:MM:SS.mmm LEVEL  module.name                    message``

    Continuation lines are indented to align with the message column.
    Module name is extracted from ``_tags`` and shortened for readability.
    """

    def _span_text_parts(self, span: Record, indent: int) -> tuple[str, TextParts]:
        parts: TextParts = []

        # Timestamp
        if self._include_timestamp:
            ts = datetime.fromtimestamp(span.timestamp / ONE_SECOND_IN_NANOSECONDS)
            ts_str = f"{ts:%H:%M:%S.%f}"[:-3]
            parts += [(ts_str, "green"), (" ", "")]

        # Level (fixed 5 chars)
        level: int = span.level
        level_name = _LEVEL_NAMES.get(level, f"L{level:<3}")
        if level >= _ERROR_LEVEL:
            level_style = "red"
        elif level >= _WARN_LEVEL:
            level_style = "yellow"
        else:
            level_style = "dim"
        parts += [(level_name, level_style), ("  ", "")]

        # Module tag in brackets, fixed width
        if self._include_tags:
            tags = span.attributes.get(ATTRIBUTES_TAGS_KEY)
            if tags:
                tag = cast("list[str]", tags)[0]
                short = _shorten_module(tag)
                bracketed = f"[{short}]"
                parts += [(f"{bracketed:<{_MODULE_WIDTH + 2}}", "cyan"), (" ", "")]
            else:
                parts += [(" " * (_MODULE_WIDTH + 3), "")]

        if indent:
            parts += [(indent * "  ", "")]

        # Message with aligned continuation lines
        msg: str = span.message
        pad = " " * _PREFIX_WIDTH
        msg = msg.replace("\n", "\n" + pad)

        if level >= _ERROR_LEVEL:
            parts += [(msg, "red")]
        elif level >= _WARN_LEVEL:
            parts += [(msg, "yellow")]
        else:
            parts += [(msg, "")]

        return msg, parts


def _shorten_module(name: str) -> str:
    """Shorten module path to fit ~20 chars.

    ``osa.domain.ingest.handler.run_ingester`` → ``ingest.run_ingester``
    ``osa.infrastructure.oci.runner``           → ``infra.oci.runner``
    ``osa.domain.feature.handler.insert_batch_features`` → ``feat.ins_batch_feat``
    """
    short = (
        name.replace("osa.domain.", "")
        .replace("osa.infrastructure.", "infra.")
        .replace(".handler.", ".")
        .replace(".service.", ".")
        .replace(".util.", ".")
    )
    if len(short) <= _MODULE_WIDTH:
        return short
    # Truncate: keep first and last segment, abbreviate middle
    parts = short.split(".")
    if len(parts) <= 2:
        return short[:_MODULE_WIDTH]
    # Keep first and last, drop middle segments until it fits
    first, *middle, last = parts
    while middle and len(f"{first}.{'.'.join(middle)}.{last}") > _MODULE_WIDTH:
        middle.pop(0)
    result = f"{first}.{'.'.join(middle)}.{last}" if middle else f"{first}.{last}"
    return result[:_MODULE_WIDTH]


class Logger:
    """Structured logger that wraps logfire with automatic module tagging.

    Usage::

        from osa.infrastructure.logging import get_logger

        log = get_logger(__name__)
        log.info("batch {idx}: pulled {n} records", idx=0, n=101)

    Produces structured logfire spans with ``_tags=[module_name]``,
    preserving key-value attributes for logfire cloud while showing
    the module name in console output.
    """

    __slots__ = ("_tags",)

    def __init__(self, name: str) -> None:
        self._tags = [name]

    def info(self, msg: str, **kwargs: Any) -> None:
        _logfire.info(msg, _tags=self._tags, **kwargs)

    def warn(self, msg: str, **kwargs: Any) -> None:
        _logfire.warn(msg, _tags=self._tags, **kwargs)

    def error(self, msg: str, **kwargs: Any) -> None:
        _logfire.error(msg, _tags=self._tags, **kwargs)

    def debug(self, msg: str, **kwargs: Any) -> None:
        _logfire.debug(msg, _tags=self._tags, **kwargs)


def get_logger(name: str) -> Logger:
    """Create a structured logger for a module.

    Args:
        name: Module name, typically ``__name__``.
    """
    return Logger(name)
