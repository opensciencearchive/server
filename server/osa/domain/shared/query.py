"""Query and QueryHandler base classes with authorization gate."""

from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from typing import Any, ClassVar, Generic, TypeVar, dataclass_transform

from pydantic import BaseModel


class Query(BaseModel):
    __public__: ClassVar[bool] = False


class Result(BaseModel): ...


C = TypeVar("C", bound=Query)
R = TypeVar("R", bound=Result)

# Unbound async handler method: (self, cmd) -> Coroutine -> Result
_HandlerMethod = Callable[..., Coroutine[Any, Any, Any]]


def _wrap_query_run_with_auth(cls: type, original_run: _HandlerMethod) -> _HandlerMethod:
    """Wrap the run() method with __auth__ policy evaluation."""

    @wraps(original_run)
    async def auth_wrapped_run(self: Any, cmd: Any) -> Any:
        from osa.domain.shared.error import AuthorizationError

        # Check if the query type is public
        cmd_type = type(cmd)
        if getattr(cmd_type, "__public__", False):
            return await original_run(self, cmd)

        # Non-public: check auth
        auth_policy = getattr(type(self), "__auth__", None)
        if auth_policy is None:
            from osa.domain.shared.error import ConfigurationError

            raise ConfigurationError(
                f"Handler {type(self).__name__} has no __auth__ declaration "
                f"and its query is not __public__"
            )

        principal = getattr(self, "_principal", None)
        if principal is None:
            raise AuthorizationError(
                "Authentication required",
                code="missing_token",
            )

        if not auth_policy.evaluate(principal):
            raise AuthorizationError(
                f"Access denied: insufficient role for {type(self).__name__}",
                code="access_denied",
            )

        return await original_run(self, cmd)

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
            __auth__ = requires_role(Role.ADMIN)
            _principal: Principal | None = None
    """

    @abstractmethod
    async def run(self, cmd: C) -> R: ...
