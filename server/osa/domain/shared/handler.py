"""The single home of handler mechanics: auth-gate wrapping + the metaclass.

``CommandHandler`` and ``QueryHandler`` are thin facades over this module —
they differ only in their DTO base (``Command`` vs ``Query``) and their result
TypeVar bound. Everything they share lives here exactly once, so the gate
semantics cannot fork between the read and write sides (arch-survey
2026-08-16 F2: the wrapper had been duplicated and had already drifted).

Gate evaluation is deliberately log-free on the request path: the startup
validator prints the full gate table once, and denials raise typed
``AuthorizationError``s that the central error mapper records.
"""

from __future__ import annotations

from abc import ABCMeta
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from typing import Any, dataclass_transform

from pydantic import BaseModel


class Result(BaseModel): ...


# Unbound async handler method: (self, cmd) -> Coroutine -> result
HandlerMethod = Callable[..., Coroutine[Any, Any, Any]]


def wrap_run_with_auth(cls: type, original_run: HandlerMethod) -> HandlerMethod:
    """Wrap a handler's ``run()`` with ``__auth__`` gate evaluation."""

    @wraps(original_run)
    async def auth_wrapped_run(self: Any, cmd: Any) -> Any:
        from osa.domain.shared.authorization.gate import AtLeast, Gate, Public, RequiresScope
        from osa.domain.shared.error import AuthorizationError, ConfigurationError

        auth_gate = getattr(type(self), "__auth__", None)

        if not isinstance(auth_gate, Gate):
            raise ConfigurationError(f"Handler {type(self).__name__} has no __auth__ declaration")

        if isinstance(auth_gate, Public):
            return await original_run(self, cmd)

        if isinstance(auth_gate, AtLeast):
            from osa.domain.auth.model.principal import Principal

            principal = getattr(self, "principal", None)
            if not isinstance(principal, Principal):
                raise AuthorizationError(
                    "Authentication required",
                    code="missing_token",
                )

            if not principal.has_role(auth_gate.role):
                raise AuthorizationError(
                    f"Access denied: insufficient role for {type(self).__name__}",
                    code="access_denied",
                )

            return await original_run(self, cmd)

        if isinstance(auth_gate, RequiresScope):
            from osa.domain.auth.model.principal import Principal
            from osa.domain.auth.model.role import Role

            principal = getattr(self, "principal", None)
            if not isinstance(principal, Principal):
                raise AuthorizationError(
                    "Authentication required",
                    code="missing_token",
                )

            if not (principal.has_scope(auth_gate.scope) or principal.has_role(Role.ADMIN)):
                raise AuthorizationError(
                    f"Access denied: missing scope {auth_gate.scope!r} for {type(self).__name__}",
                    code="access_denied",
                )

            return await original_run(self, cmd)

        raise ConfigurationError(  # pragma: no cover — future gate types handled here
            f"Handler {type(self).__name__} has unhandled __auth__ type: {type(auth_gate).__name__}"
        )

    return auth_wrapped_run


@dataclass_transform()
class HandlerMeta(ABCMeta):
    """ABC + auto-dataclass + ``__auth__`` gate wrap, applied to subclasses only.

    The facade classes themselves (``CommandHandler``/``QueryHandler``) have no
    base carrying this metaclass, so they are left untouched; every concrete
    handler subclass is dataclass-ified and gate-wrapped.
    """

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)
        if any(isinstance(b, mcs) for b in bases):
            cls = dataclass(cls)

            original_run = cls.__dict__.get("run")
            if original_run is not None:
                cls.run = wrap_run_with_auth(cls, original_run)

        return cls
