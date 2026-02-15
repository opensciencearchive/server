"""Query and QueryHandler base classes with authorization gate."""

from __future__ import annotations

from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, dataclass_transform

from pydantic import BaseModel

if TYPE_CHECKING:
    from osa.domain.shared.authorization.gate import Gate


class Query(BaseModel): ...


class Result(BaseModel): ...


C = TypeVar("C", bound=Query)
R = TypeVar("R", bound=Result)

# Unbound async handler method: (self, cmd) -> Coroutine -> Result
_HandlerMethod = Callable[..., Coroutine[Any, Any, Any]]


def _wrap_query_run_with_auth(cls: type, original_run: _HandlerMethod) -> _HandlerMethod:
    """Wrap the run() method with __auth__ gate evaluation."""

    @wraps(original_run)
    async def auth_wrapped_run(self: Any, cmd: Any) -> Any:
        from osa.domain.shared.authorization.gate import AtLeast, Gate, Public
        from osa.domain.shared.error import AuthorizationError, ConfigurationError

        auth_gate = getattr(type(self), "__auth__", None)

        if not isinstance(auth_gate, Gate):
            raise ConfigurationError(f"Handler {type(self).__name__} has no __auth__ declaration")

        if isinstance(auth_gate, Public):
            return await original_run(self, cmd)

        if isinstance(auth_gate, AtLeast):
            import logging as _logging

            from osa.domain.auth.model.principal import Principal

            _auth_logger = _logging.getLogger("osa.authz")

            principal = getattr(self, "principal", None)
            if not isinstance(principal, Principal):
                raise AuthorizationError(
                    "Authentication required",
                    code="missing_token",
                )

            _auth_logger.debug(
                "Auth check: handler=%s, required=%s, principal_roles=%s, user_id=%s",
                type(self).__name__,
                auth_gate.role,
                principal.roles,
                principal.user_id,
            )

            if not principal.has_role(auth_gate.role):
                raise AuthorizationError(
                    f"Access denied: insufficient role for {type(self).__name__}",
                    code="access_denied",
                )

            return await original_run(self, cmd)

        raise ConfigurationError(  # pragma: no cover — future gate types handled here
            f"Handler {type(self).__name__} has unhandled __auth__ type: {type(auth_gate).__name__}"
        )

    return auth_wrapped_run


@dataclass_transform()
class _QueryHandlerMeta(ABCMeta):
    """Metaclass that combines ABC with auto-dataclass and __auth__ gate for subclasses."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)
        if any(isinstance(b, mcs) for b in bases):
            cls = dataclass(cls)

            # Wrap run() with auth gate
            original_run = cls.__dict__.get("run")
            if original_run is not None:
                wrapped = _wrap_query_run_with_auth(cls, original_run)
                cls.run = wrapped

        return cls


class QueryHandler(Generic[C, R], metaclass=_QueryHandlerMeta):
    """Base class for query handlers. Subclasses are automatically dataclasses.

    Declare __auth__ to enforce role-based access:
        class MyHandler(QueryHandler[MyQuery, MyResult]):
            __auth__ = at_least(Role.ADMIN)
            principal: Principal
    """

    __auth__: ClassVar[Gate]

    @abstractmethod
    async def run(self, cmd: C) -> R: ...
