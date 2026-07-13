"""Startup validation for handler authorization declarations."""

import dataclasses
import logging
from collections.abc import Iterator

from dishka import AsyncContainer

from osa.domain.shared.authorization.gate import AtLeast, Gate, RequiresScope
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

    if isinstance(auth, (AtLeast, RequiresScope)):
        field_names = (
            {f.name for f in dataclasses.fields(handler_cls)}
            if dataclasses.is_dataclass(handler_cls)
            else set()
        )
        if "principal" not in field_names:
            gate_name = "at_least(...)" if isinstance(auth, AtLeast) else "requires_scope(...)"
            raise ConfigurationError(
                f"Handler {handler_cls.__name__} declares __auth__ = {gate_name} "
                "but is missing a `principal: Principal` field. Without it the "
                "auth gate rejects every request with a misleading 'missing_token' "
                "even when the caller's JWT is valid."
            )


def _registered_handler_classes(container: AsyncContainer) -> Iterator[type]:
    """Every CommandHandler/QueryHandler type Dishka can actually construct.

    Walks the container's registry chain — one ``Registry`` per DI scope,
    linked via ``child_registry`` — reading each factory's declared
    ``provides.type_hint`` rather than calling ``CommandHandler.__subclasses__()``.
    This set is exactly "what create_container() wired up for this app," so it
    can't be polluted by handler subclasses a test defines transiently: a class
    only appears here if some Provider actually calls ``provide(...)`` on it,
    regardless of how long the class object itself lingers in memory.

    Trade-off, deliberately accepted: this is a NARROWER set than "every class
    that ever subclassed CommandHandler/QueryHandler in this process." A
    handler that's written but never wired into any Provider — dead code, or a
    handler added and forgotten before its DI registration — used to be caught
    here (it still shows up in ``__subclasses__()``) and now silently isn't,
    because it's unreachable from the container and therefore invisible to
    this scan. That handler can't be resolved or run either way, so the
    practical risk is low, but it is a real behavior change: a bug class this
    check used to catch for free (a handler that exists but was never
    actually wired in) now goes undetected here, in some hard-to-predict way
    depending on what else could have surfaced the same gap. Worth revisiting
    if "boots fine, but this handler is silently never reachable" ever shows
    up in an incident.
    """
    seen: set[type] = set()
    registry = container.registry
    while registry is not None:
        for factory in registry.factories.values():
            hint = factory.provides.type_hint
            if (
                isinstance(hint, type)
                and issubclass(hint, (CommandHandler, QueryHandler))
                and hint not in seen
            ):
                seen.add(hint)
                yield hint
        registry = registry.child_registry


def validate_all_handlers(container: AsyncContainer) -> None:
    """Check every CommandHandler/QueryHandler Dishka can construct for __auth__.

    Raises ConfigurationError listing all handlers missing __auth__ declarations.
    """
    violations: list[str] = []

    for handler_cls in _registered_handler_classes(container):
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
