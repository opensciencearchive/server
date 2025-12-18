from dishka import Scope, provide

from osa.domain.shared.port.event_bus import EventBus
from osa.infrastructure.event.memory_bus import InMemoryEventBus
from osa.util.di.base import Provider


class SharedProvider(Provider):
    @provide(scope=Scope.APP)
    def get_event_bus(self) -> EventBus:
        # Singleton bus for in-memory comms
        return InMemoryEventBus()
