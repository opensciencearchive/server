import asyncio
import logging
from collections import defaultdict
from typing import Callable, Type, Awaitable

from osa.domain.shared.event import Event
from osa.domain.shared.port.event_bus import EventBus

logger = logging.getLogger(__name__)

EventHandlerFunc = Callable[[Event], Awaitable[None]]


class InMemoryEventBus(EventBus):
    def __init__(self):
        self._subscribers: dict[Type[Event], list[EventHandlerFunc]] = defaultdict(list)

    def subscribe(self, event_type: Type[Event], handler: EventHandlerFunc) -> None:
        self._subscribers[event_type].append(handler)

    async def publish(self, event: Event) -> None:
        event_type = type(event)
        handlers = self._subscribers.get(event_type, [])
        
        if not handlers:
            logger.debug(f"No handlers for event {event_type.__name__}")
            return

        logger.info(f"Publishing event {event_type.__name__} to {len(handlers)} handlers")
        
        # concurrent execution
        await asyncio.gather(*[h(event) for h in handlers])
