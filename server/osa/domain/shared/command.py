from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
from typing import Generic, TypeVar, dataclass_transform

from pydantic import BaseModel


class Command(BaseModel): ...


class Result(BaseModel): ...


C = TypeVar("C", bound=Command)
R = TypeVar("R", bound=Result)


@dataclass_transform()
class _CommandHandlerMeta(ABCMeta):
    """Metaclass that combines ABC with auto-dataclass for subclasses."""

    def __new__(mcs, name: str, bases: tuple, namespace: dict):
        cls = super().__new__(mcs, name, bases, namespace)
        if any(isinstance(b, mcs) for b in bases):
            return dataclass(cls)
        return cls


class CommandHandler(Generic[C, R], metaclass=_CommandHandlerMeta):
    """Base class for command handlers. Subclasses are automatically dataclasses."""

    @abstractmethod
    async def run(self, cmd: C) -> R: ...
