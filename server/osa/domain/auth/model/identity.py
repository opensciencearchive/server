"""Identity hierarchy — base types for all request identities."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Identity:
    """Base for all request identities."""

    pass


@dataclass(frozen=True)
class Anonymous(Identity):
    """Unauthenticated request."""

    pass


@dataclass(frozen=True)
class System(Identity):
    """Internal worker/background process. Bypasses resource checks."""

    pass
