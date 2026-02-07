"""Composable policy types for handler-level authorization gates."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from osa.domain.auth.model.principal import Principal
    from osa.domain.auth.model.role import Role


class Policy(ABC):
    """Base class for composable authorization policies.

    Policies are evaluated at the handler level as a coarse pre-filter
    (role check only, no resource loaded yet).
    """

    @abstractmethod
    def evaluate(self, principal: "Principal") -> bool:
        """Return True if principal satisfies this policy."""
        ...

    def __and__(self, other: Policy) -> AllOf:
        return AllOf(policies=(self, other))

    def __or__(self, other: Policy) -> AnyOf:
        return AnyOf(policies=(self, other))

    def __invert__(self) -> Not:
        return Not(policy=self)


@dataclass(frozen=True)
class RequiresRole(Policy):
    """Policy that checks principal has at least the given role (hierarchy)."""

    role: "Role"

    def evaluate(self, principal: "Principal") -> bool:
        return principal.has_role(self.role)


@dataclass(frozen=True)
class AllOf(Policy):
    """Policy that requires ALL sub-policies to pass."""

    policies: tuple[Policy, ...]

    def evaluate(self, principal: "Principal") -> bool:
        return all(p.evaluate(principal) for p in self.policies)


@dataclass(frozen=True)
class AnyOf(Policy):
    """Policy that requires at least ONE sub-policy to pass."""

    policies: tuple[Policy, ...]

    def evaluate(self, principal: "Principal") -> bool:
        return any(p.evaluate(principal) for p in self.policies)


@dataclass(frozen=True)
class Not(Policy):
    """Policy that inverts another policy."""

    policy: Policy

    def evaluate(self, principal: "Principal") -> bool:
        return not self.policy.evaluate(principal)


def requires_role(role: "Role") -> RequiresRole:
    """Factory: policy requiring at least the given role."""
    return RequiresRole(role=role)


def requires_any_role(*roles: "Role") -> AnyOf:
    """Factory: policy requiring at least one of the given roles."""
    return AnyOf(policies=tuple(RequiresRole(role=r) for r in roles))
