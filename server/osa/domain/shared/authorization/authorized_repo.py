"""AuthorizedRepo — wraps a raw repository, returns Guarded[T] from get()."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, TypeVar

from osa.domain.shared.authorization.guarded import Guarded
from osa.domain.shared.error import NotFoundError

if TYPE_CHECKING:
    from osa.domain.auth.model.principal import Principal
    from osa.domain.shared.authorization.policy_set import PolicySet

T = TypeVar("T")
ID = TypeVar("ID")


class AuthorizedRepo(Generic[T, ID]):
    """Wraps a raw repository and returns Guarded[T] from get().

    Used by services that need to enforce authorization on loaded resources.
    Event handlers and background workers should use the raw repository directly.
    """

    def __init__(
        self,
        inner: Any,
        principal: "Principal",
        policy_set: "PolicySet",
    ) -> None:
        self._inner = inner
        self._principal = principal
        self._policy_set = policy_set

    async def get(self, id: ID) -> Guarded[T]:
        """Load a resource and wrap it in Guarded[T]."""
        resource = await self._inner.get(id)
        if resource is None:
            raise NotFoundError(f"Resource not found: {id}")
        return Guarded(resource, self._principal, self._policy_set)
