"""Guarded[T] — generic wrapper forcing explicit authorization check."""

from __future__ import annotations

from typing import TYPE_CHECKING, Generic, TypeVar

from osa.domain.shared.authorization.action import Action

if TYPE_CHECKING:
    from osa.domain.auth.model.principal import Principal
    from osa.domain.shared.authorization.policy_set import PolicySet

T = TypeVar("T")


class Guarded(Generic[T]):
    """Wraps a loaded domain resource, forcing an explicit authorization check.

    The ONLY way to access the inner resource is via `.check(action)`.
    No attribute proxy — accessing attributes on Guarded raises AttributeError.
    """

    __slots__ = ("_resource", "_principal", "_policy_set")

    def __init__(
        self,
        resource: T,
        principal: Principal,
        policy_set: PolicySet,
    ) -> None:
        self._resource = resource
        self._principal = principal
        self._policy_set = policy_set

    def check(self, action: Action) -> T:
        """Evaluate authorization and return the unwrapped resource.

        Raises AuthorizationError if access is denied.
        """
        self._policy_set.guard(self._principal, action, self._resource)
        return self._resource
