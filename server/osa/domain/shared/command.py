"""Command and CommandHandler base classes with authorization gate.

The gate wrapper and metaclass live once in :mod:`osa.domain.shared.handler`;
this module contributes only the write-side vocabulary: the ``Command`` DTO
base and the ``Result``-bound result TypeVar.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, ClassVar, Generic, TypeVar

from pydantic import BaseModel

from osa.domain.shared.handler import HandlerMeta, Result

if TYPE_CHECKING:
    from osa.domain.shared.authorization.gate import Gate

__all__ = ["Command", "CommandHandler", "Result"]


class Command(BaseModel): ...


C = TypeVar("C", bound=Command)
R = TypeVar("R", bound=Result)


class CommandHandler(Generic[C, R], metaclass=HandlerMeta):
    """Base class for command handlers. Subclasses are automatically dataclasses.

    Declare __auth__ to enforce role-based access:
        class MyHandler(CommandHandler[MyCmd, MyResult]):
            __auth__ = at_least(Role.ADMIN)
            principal: Principal
    """

    __auth__: ClassVar[Gate]

    @abstractmethod
    async def run(self, cmd: C) -> R: ...
