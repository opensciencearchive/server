from abc import ABC, abstractmethod
from types import TracebackType
from typing import Optional, Type


class UoW(ABC):
    @abstractmethod
    def commit(self): ...

    @abstractmethod
    def rollback(self): ...

    def __enter__(self) -> "UoW":
        return self

    # TODO: is this signature correct?
    def __exit__(
        self,
        exc_type: Optional[Type[Exception]] = None,
        exc: Optional[Exception] = None,
        tb: Optional[TracebackType] = None,
    ):
        self.rollback() if exc else self.commit()
