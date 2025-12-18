from abc import abstractmethod
from typing import Protocol

from osa.domain.shared.event import Event


class EventBus(Protocol):

    @abstractmethod
    async def publish(self, event: Event) -> None:
        ...
