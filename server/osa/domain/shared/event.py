"""Domain events, event handlers, scheduled tasks, and worker infrastructure."""

from abc import ABC, ABCMeta, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import (
    Any,
    ClassVar,
    Generic,
    Iterator,
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


# --- Worker Infrastructure ---


@dataclass(frozen=True)
class WorkerConfig:
    """Configuration for a single worker instance.

    Attributes:
        name: Unique worker identifier.
        event_types: Event types to claim.
        routing_key: Optional routing key filter.
        batch_size: Max events per batch (default: 1).
        batch_timeout: Max seconds to wait for batch (default: 5.0).
        poll_interval: Seconds between polls when idle (default: 0.5).
        max_retries: Max retry attempts before marking failed (default: 3).
        claim_timeout: Seconds before claim considered stale (default: 300.0).
    """

    name: str
    event_types: tuple[type["Event"], ...]
    routing_key: str | None = None
    batch_size: int = 1
    batch_timeout: float = 5.0
    poll_interval: float = 0.5
    max_retries: int = 3
    claim_timeout: float = 300.0

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if not self.event_types:
            raise ValueError("event_types must not be empty")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.batch_timeout <= 0:
            raise ValueError("batch_timeout must be > 0")
        if self.poll_interval <= 0:
            raise ValueError("poll_interval must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.claim_timeout <= self.batch_timeout:
            raise ValueError("claim_timeout must be > batch_timeout")


class WorkerStatus(Enum):
    """Status of a running worker."""

    IDLE = "idle"
    CLAIMING = "claiming"
    PROCESSING = "processing"
    STOPPING = "stopping"


@dataclass
class WorkerState:
    """Runtime state for a running worker (not persisted).

    Attributes:
        config: Worker configuration.
        status: Current worker status.
        current_batch: Events currently being processed.
        last_claim_at: When last claim was made.
        processed_count: Total events processed.
        failed_count: Total events failed.
        error: Last error if any.
    """

    config: WorkerConfig
    status: WorkerStatus = WorkerStatus.IDLE
    current_batch: list["Event"] = field(default_factory=list)
    last_claim_at: datetime | None = None
    processed_count: int = 0
    failed_count: int = 0
    error: Exception | None = None


@dataclass(frozen=True)
class ClaimResult:
    """Result of a claim operation.

    Attributes:
        events: Claimed events (locked).
        claimed_at: Timestamp of claim.
    """

    events: list["Event"]
    claimed_at: datetime

    def __bool__(self) -> bool:
        """Return True if events are present."""
        return len(self.events) > 0

    def __len__(self) -> int:
        """Return number of events."""
        return len(self.events)

    def __iter__(self) -> Iterator["Event"]:
        """Iterate over events."""
        return iter(self.events)


# --- EventHandler ---


def _extract_event_type(cls: type) -> type["Event"] | None:
    """Extract the event type E from EventHandler[E] in class bases."""
    for base in getattr(cls, "__orig_bases__", []):
        origin = get_origin(base)
        origin_name = getattr(origin, "__name__", None)
        if origin is not None and origin_name == "EventHandler":
            args = get_args(base)
            if args and isinstance(args[0], type) and issubclass(args[0], Event):
                return args[0]
    return None


@dataclass_transform()
class _EventHandlerMeta(ABCMeta):
    """Metaclass that applies @dataclass and extracts __event_type__ from EventHandler[E]."""

    def __new__(mcs, name: str, bases: tuple[type, ...], namespace: dict[str, Any]) -> type:
        cls = super().__new__(mcs, name, bases, namespace)
        # Apply dataclass and extract event type for concrete subclasses
        if any(isinstance(b, mcs) for b in bases):
            cls = dataclass(cls)
            event_type = _extract_event_type(cls)
            if event_type is not None:
                cls.__event_type__ = event_type
        return cls


class EventHandler(Generic[E], metaclass=_EventHandlerMeta):
    """Base class for pull-based event handlers.

    EventHandler replaces both EventListener and BatchEventListener with a unified pattern.
    Workers claim events from the outbox and delegate to handlers for processing.

    Subclasses are automatically dataclasses with DI-injected dependencies.
    The __event_type__ is extracted from the generic parameter.

    Configuration is via class variables:
        __routing_key__: Optional filter for routing key (default: None)
        __batch_size__: Max events to claim at once (default: 1)
        __batch_timeout__: Timeout for partial batches in seconds (default: 5.0)
        __poll_interval__: Seconds between polls when idle (default: 0.5)
        __max_retries__: Max retry attempts before marking failed (default: 3)
        __claim_timeout__: Seconds before claim considered stale (default: 300.0)

    Example (single event):
        class TriggerInitialSourceRun(EventHandler[ServerStarted]):
            _config: Config
            _outbox: Outbox

            async def handle(self, event: ServerStarted) -> None:
                for source in self._config.sources:
                    if source.initial_run and source.initial_run.enabled:
                        await self._outbox.append(SourceRequested(...))

    Example (batch processing):
        class VectorIndexHandler(EventHandler[IndexRecord]):
            __routing_key__ = "vector"
            __batch_size__ = 100
            __batch_timeout__ = 5.0

            _backend: VectorStorageBackend

            async def handle_batch(self, events: list[IndexRecord]) -> None:
                records = [(str(e.record_srn), e.metadata) for e in events]
                await self._backend.ingest_batch(records)
    """

    __event_type__: ClassVar[type[Event]]
    __routing_key__: ClassVar[str | None] = None
    __batch_size__: ClassVar[int] = 1
    __batch_timeout__: ClassVar[float] = 5.0
    __poll_interval__: ClassVar[float] = 0.5
    __max_retries__: ClassVar[int] = 3
    __claim_timeout__: ClassVar[float] = 300.0

    async def handle(self, event: E) -> None:
        """Handle a single event. Override for single-event processing.

        Args:
            event: The event to handle.

        Raises:
            NotImplementedError: If neither handle() nor handle_batch() is overridden.
        """
        raise NotImplementedError(
            f"{type(self).__name__} must implement handle() or handle_batch()"
        )

    async def handle_batch(self, events: list[E]) -> None:
        """Handle a batch of events. Override for batch processing.

        Default implementation loops over handle() for each event.
        Override for more efficient batch operations.

        Args:
            events: List of events to handle (all same type).
        """
        for event in events:
            await self.handle(event)


# --- Schedule ---


@dataclass
class Schedule(ABC):
    """Base class for scheduled tasks.

    Subclasses are dataclasses with DI-injected dependencies.
    The cron expression is provided via config, not on the class.

    Example:
        @dataclass
        class SourceSchedule(Schedule):
            outbox: Outbox

            async def run(self, source_name: str, limit: int | None = None) -> None:
                last_run = await self.outbox.find_latest(SourceRunCompleted)
                await self.outbox.append(SourceRequested(...))
    """

    @abstractmethod
    async def run(self, **params: Any) -> None:
        """Run the scheduled task with parameters from config."""
        ...
