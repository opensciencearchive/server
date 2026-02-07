"""Command and CommandHandler base classes with authorization gate."""

from abc import ABCMeta, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from functools import wraps
from typing import Any, ClassVar, Generic, TypeVar, dataclass_transform

from pydantic import BaseModel


class Command(BaseModel):
    __public__: ClassVar[bool] = False


class Result(BaseModel): ...


C = TypeVar("C", bound=Command)
R = TypeVar("R", bound=Result)

# Unbound async handler method: (self, cmd) -> Coroutine -> Result
_HandlerMethod = Callable[..., Coroutine[Any, Any, Any]]


def _get_command_type(cls: type) -> type[Command] | None:
    """Extract the Command type C from CommandHandler[C, R] in class bases."""
    from typing import get_args, get_origin

    for base in getattr(cls, "__orig_bases__", []):
        origin = get_origin(base)
        if origin is not None and getattr(origin, "__name__", None) == "CommandHandler":
            args = get_args(base)
            if args and isinstance(args[0], type) and issubclass(args[0], Command):
                return args[0]
    return None


def _wrap_run_with_auth(cls: type, original_run: _HandlerMethod) -> _HandlerMethod:
    """Wrap the run() method with __auth__ policy evaluation."""

    @wraps(original_run)
    async def auth_wrapped_run(self: Any, cmd: Any) -> Any:
        from osa.domain.shared.error import AuthorizationError

        # Check if the command type is public
        cmd_type = type(cmd)
        if getattr(cmd_type, "__public__", False):
            return await original_run(self, cmd)

        # Non-public: check auth
        auth_policy = getattr(type(self), "__auth__", None)
        if auth_policy is None:
            from osa.domain.shared.error import ConfigurationError

            raise ConfigurationError(
                f"Handler {type(self).__name__} has no __auth__ declaration "
                f"and its command is not __public__"
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
class _CommandHandlerMeta(ABCMeta):
    """Metaclass that combines ABC with auto-dataclass and __auth__ gate for subclasses."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]):
        cls = super().__new__(mcs, name, bases, namespace)
        if any(isinstance(b, mcs) for b in bases):
            cls = dataclass(cls)

            # Wrap run() with auth gate if __auth__ is declared or command is not public
            original_run = cls.__dict__.get("run")
            if original_run is not None:
                wrapped = _wrap_run_with_auth(cls, original_run)
                cls.run = wrapped

        return cls


class CommandHandler(Generic[C, R], metaclass=_CommandHandlerMeta):
    """Base class for command handlers. Subclasses are automatically dataclasses.

    Declare __auth__ to enforce role-based access:
        class MyHandler(CommandHandler[MyCmd, MyResult]):
            __auth__ = requires_role(Role.ADMIN)
            _principal: Principal | None = None
    """

    @abstractmethod
    async def run(self, cmd: C) -> R: ...
