from typing import Generic, TypeVar
from pydantic import BaseModel, RootModel

T = TypeVar("T")


class ValueObject(BaseModel): ...

class RootValueObject(RootModel[T], Generic[T]): ...
