"""Handler-level authorization gates: public(), at_least(Role), requires_scope(str)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from osa.domain.auth.model.role import Role


class Gate:
    """Base for handler-level authorization gates.

    Every CommandHandler/QueryHandler must declare ``__auth__: ClassVar[Gate]``.
    Subclasses define specific gate behaviors (public access, role checks, etc.).
    """


@dataclass(frozen=True)
class Public(Gate):
    """No authentication required."""


@dataclass(frozen=True)
class AtLeast(Gate):
    """Gate that requires the principal to have at least the given role."""

    role: "Role"


@dataclass(frozen=True)
class RequiresScope(Gate):
    """Gate for machine (M2M) credentials (#145, US5).

    Authorizes if the principal holds the named OAuth scope **or** is an ADMIN.
    The ADMIN fallback keeps every scoped endpoint reachable with an ordinary
    admin user token, so scopes narrow — never replace — the role model.
    """

    scope: str


_PUBLIC = Public()


def public() -> Public:
    """Mark a handler as publicly accessible (no auth required)."""
    return _PUBLIC


def at_least(role: "Role") -> AtLeast:
    """Mark a handler as requiring at least the given role."""
    return AtLeast(role=role)


def requires_scope(scope: str) -> RequiresScope:
    """Mark a handler as requiring an OAuth scope (or ADMIN). See RequiresScope."""
    return RequiresScope(scope=scope)
