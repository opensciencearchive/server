from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict

from osa.domain.shared.model.srn import EventSRN


class Event(BaseModel, ABC):
    srn: EventSRN


E = TypeVar("E", bound=Event)


class EventListener(BaseModel, Generic[E], ABC):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

    @abstractmethod
    def handle(self, event: E) -> None: ...


class EventBus(ABC): ...
