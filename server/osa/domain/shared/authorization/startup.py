"""Startup validation for handler authorization declarations."""

import logging

from osa.domain.shared.authorization.gate import Gate
from osa.domain.shared.command import CommandHandler
from osa.domain.shared.error import ConfigurationError
from osa.domain.shared.query import QueryHandler

logger = logging.getLogger(__name__)


def _check_handler_class(handler_cls: type) -> None:
    """Check a single handler class for __auth__ declaration.

    Every handler must have __auth__ set to a Gate instance.
    """
    auth = getattr(handler_cls, "__auth__", None)
    if not isinstance(auth, Gate):
        raise ConfigurationError(f"Handler {handler_cls.__name__} has no __auth__ declaration")


def validate_all_handlers() -> None:
    """Scan all registered CommandHandler and QueryHandler subclasses.

    Raises ConfigurationError listing all handlers missing __auth__ declarations.
    """
    violations: list[str] = []

    for handler_cls in CommandHandler.__subclasses__():
        try:
            _check_handler_class(handler_cls)
        except ConfigurationError as e:
            violations.append(str(e))

    for handler_cls in QueryHandler.__subclasses__():
        try:
            _check_handler_class(handler_cls)
        except ConfigurationError as e:
            violations.append(str(e))

    if violations:
        raise ConfigurationError(
            f"Authorization validation failed for {len(violations)} handler(s):\n"
            + "\n".join(f"  - {v}" for v in violations)
        )

    logger.info("Authorization startup validation passed for all handlers")
