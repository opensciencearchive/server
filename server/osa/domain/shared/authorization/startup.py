"""Startup validation for handler authorization declarations."""

import logging

from osa.domain.shared.command import CommandHandler
from osa.domain.shared.error import ConfigurationError
from osa.domain.shared.query import QueryHandler

logger = logging.getLogger(__name__)


def _get_command_or_query_type(handler_cls: type) -> type | None:
    """Extract the Command/Query type from a handler's generic bases."""
    from typing import get_args, get_origin

    for base in getattr(handler_cls, "__orig_bases__", []):
        origin = get_origin(base)
        if origin is None:
            continue
        name = getattr(origin, "__name__", "")
        if name in ("CommandHandler", "QueryHandler"):
            args = get_args(base)
            if args and isinstance(args[0], type):
                return args[0]
    return None


def _check_handler_class(handler_cls: type, dto_cls: type | None = None) -> None:
    """Check a single handler class for __auth__ declaration.

    Raises ConfigurationError if the handler lacks __auth__ and its DTO is not __public__.
    """
    if dto_cls is None:
        dto_cls = _get_command_or_query_type(handler_cls)

    # If DTO is public, no __auth__ needed
    if dto_cls is not None and getattr(dto_cls, "__public__", False):
        return

    # Check for __auth__
    if not hasattr(handler_cls, "__auth__") or getattr(handler_cls, "__auth__") is None:
        raise ConfigurationError(
            f"Handler {handler_cls.__name__} has no __auth__ declaration "
            f"and its command/query is not __public__"
        )


def validate_all_handlers() -> None:
    """Scan all registered CommandHandler and QueryHandler subclasses.

    Raises ConfigurationError listing all handlers missing __auth__ declarations.
    """
    violations: list[str] = []

    for handler_cls in CommandHandler.__subclasses__():
        dto_cls = _get_command_or_query_type(handler_cls)
        try:
            _check_handler_class(handler_cls, dto_cls)
        except ConfigurationError as e:
            violations.append(str(e))

    for handler_cls in QueryHandler.__subclasses__():
        dto_cls = _get_command_or_query_type(handler_cls)
        try:
            _check_handler_class(handler_cls, dto_cls)
        except ConfigurationError as e:
            violations.append(str(e))

    if violations:
        raise ConfigurationError(
            f"Authorization validation failed for {len(violations)} handler(s):\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    logger.info("Authorization startup validation passed for all handlers")
