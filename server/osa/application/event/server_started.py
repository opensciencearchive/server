"""ServerStarted event - emitted when the server is ready."""

from osa.domain.shared.event import Event, EventId


class ServerStarted(Event):
    """Emitted once when the server starts and event system is ready.

    Listeners can use this to trigger startup tasks like:
    - Initial data ingestion
    - Cache warming
    - Federation sync
    """

    id: EventId
