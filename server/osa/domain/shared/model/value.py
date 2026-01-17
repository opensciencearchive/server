from typing import Generic, TypeVar
from pydantic import BaseModel, ConfigDict, RootModel

T = TypeVar("T")


class ValueObject(BaseModel):
    model_config = ConfigDict(frozen=True)


class RootValueObject(RootModel[T], Generic[T]):
    model_config = ConfigDict(frozen=True)
