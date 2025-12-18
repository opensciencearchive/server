from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from pydantic import BaseModel

from osa.domain.shared.model.srn import EventSRN


class Event(BaseModel, ABC):
    srn: EventSRN


E = TypeVar("E", bound=Event)


class EventListener(Generic[E], ABC):
    """Base class for event listeners."""

    @abstractmethod
    async def handle(self, event: E) -> None:
        ...

