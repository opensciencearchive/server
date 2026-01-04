"""Domain events, event listeners, and scheduled tasks."""

from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import (
    Any,
    ClassVar,
    Generic,
    NewType,
    TypeVar,
    dataclass_transform,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic import Field

from osa.domain.shared.model.entity import Entity

EventId = NewType("EventId", UUID)

E = TypeVar("E", bound="Event")


def _utc_now() -> datetime:
    return datetime.now(UTC)


class Event(Entity):
    """Base class for domain events.

    Subclasses are automatically registered by name in Event._registry.
    """

    id: EventId
    created_at: datetime = Field(default_factory=_utc_now)

    # Auto-populated registry of all Event subclasses
    _registry: ClassVar[dict[str, type["Event"]]] = {}

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        # Register concrete event types by class name
        cls._registry[cls.__name__] = cls


# --- EventListener (Subscription) ---


def _extract_event_type(cls: type) -> type["Event"] | None:
    """Extract the event type E from EventListener[E] in class bases."""
    for base in getattr(cls, "__orig_bases__", []):
        origin = get_origin(base)
        if origin is not None and getattr(origin, "__name__", None) == "EventListener":
            args = get_args(base)
            if args and isinstance(args[0], type) and issubclass(args[0], Event):
                return args[0]
    return None


@dataclass_transform()
class _EventListenerMeta(ABCMeta):
    """Metaclass that applies @dataclass and extracts __event_type__ from EventListener[E]."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> type:
        cls = super().__new__(mcs, name, bases, namespace)
        # Apply dataclass and extract event type for concrete subclasses
        if any(isinstance(b, mcs) for b in bases):
            cls = dataclass(cls)  # type: ignore[assignment]
            event_type = _extract_event_type(cls)
            if event_type is not None:
                cls.__event_type__ = event_type  # type: ignore[attr-defined]
        return cls


class EventListener(Generic[E], metaclass=_EventListenerMeta):
    """Base class for event listeners (subscriptions).

    Subclasses are automatically dataclasses and have __event_type__ set
    based on their generic parameter.

    Example:
        class IngestListener(EventListener[IngestRequested]):
            outbox: Outbox
            config: Config

            async def handle(self, event: IngestRequested) -> None:
                ...

        # IngestListener.__event_type__ == IngestRequested
    """

    __event_type__: ClassVar[type[Event]]

    @abstractmethod
    async def handle(self, event: Any) -> None:
        """Handle the event. Subclasses should type event as their specific event type."""
        ...


# --- Schedule ---


@dataclass
class Schedule(ABC):
    """Base class for scheduled tasks.

    Subclasses are dataclasses with DI-injected dependencies.
    The cron expression is provided via config, not on the class.

    Example:
        @dataclass
        class IngestSchedule(Schedule):
            outbox: Outbox

            async def run(self, ingestor_name: str, limit: int | None = None) -> None:
                last_run = await self.outbox.find_latest(IngestionRunCompleted)
                await self.outbox.append(IngestRequested(...))
    """

    @abstractmethod
    async def run(self, **params: Any) -> None:
        """Run the scheduled task with parameters from config."""
        ...
