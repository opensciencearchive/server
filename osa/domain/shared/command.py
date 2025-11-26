from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pydantic import BaseModel


# TODO: should we colocate data with behaviour, on the same class?
class Command(BaseModel, ABC): ...


class Result(BaseModel, ABC): ...


C = TypeVar("C", bound=Command)
R = TypeVar("R", bound=Result)


class CommandHandler(Generic[C, R], ABC):
    @abstractmethod
    def run(self, cmd: C) -> R: ...
