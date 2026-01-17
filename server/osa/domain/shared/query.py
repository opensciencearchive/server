from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from pydantic import BaseModel


class Query(BaseModel, ABC): ...


class Result(BaseModel, ABC): ...


C = TypeVar("C", bound=Query)
R = TypeVar("R", bound=Result)


class QueryHandler(ABC, Generic[C, R]):
    @abstractmethod
    def run(self, cmd: C) -> R: ...
