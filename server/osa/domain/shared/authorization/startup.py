"""Startup validation for handler authorization declarations."""

import dataclasses
import logging

from osa.domain.shared.authorization.gate import AtLeast, Gate
from osa.domain.shared.command import CommandHandler
from osa.domain.shared.error import ConfigurationError
from osa.domain.shared.query import QueryHandler

logger = logging.getLogger(__name__)


def _check_handler_class(handler_cls: type) -> None:
    """Check a single handler class for __auth__ declaration.

    Every handler must have __auth__ set to a Gate instance, and any handler
    with a role-based gate must declare ``principal: Principal`` so DI can
    inject it — without that field the gate reads ``getattr(self, 'principal',
    None)`` as ``None`` and rejects every request with a misleading
    ``missing_token``.
    """
    auth = getattr(handler_cls, "__auth__", None)
    if not isinstance(auth, Gate):
        raise ConfigurationError(f"Handler {handler_cls.__name__} has no __auth__ declaration")

    if isinstance(auth, AtLeast):
        field_names = (
            {f.name for f in dataclasses.fields(handler_cls)}
            if dataclasses.is_dataclass(handler_cls)
            else set()
        )
        if "principal" not in field_names:
            raise ConfigurationError(
                f"Handler {handler_cls.__name__} declares __auth__ = at_least(...) "
                "but is missing a `principal: Principal` field. Without it the "
                "auth gate rejects every request with a misleading 'missing_token' "
                "even when the caller's JWT is valid."
            )


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
