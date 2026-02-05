"""Domain events for the auth domain."""

from osa.domain.shared.event import Event, EventId


class UserAuthenticated(Event):
    """Emitted when a user successfully authenticates."""

    id: EventId
    user_id: str
    provider: str
    orcid_id: str


class UserLoggedOut(Event):
    """Emitted when a user logs out."""

    id: EventId
    user_id: str
