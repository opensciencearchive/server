"""PolicySet — declarative authorization rules and the Relationship enum.

Contains PolicyRule, Relationship, allow() constructor, and the POLICY_SET constant.
This is the single source of truth for all "who can do what on which resource" rules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import StrEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from osa.domain.auth.model.principal import Principal

from osa.domain.shared.authorization.action import Action

logger = logging.getLogger(__name__)


class Relationship(StrEnum):
    """Relationships between a principal and a resource."""

    OWNER = "owner"


@dataclass(frozen=True)
class PolicyRule:
    """A single authorization rule in the policy set."""

    action: Action
    role: "Role | None" = None
    relationship: Relationship | None = None


def allow(
    action: Action,
    *,
    role: "Role | None" = None,
    relationship: Relationship | None = None,
) -> PolicyRule:
    """Convenience constructor for a policy rule."""
    return PolicyRule(action=action, role=role, relationship=relationship)


# Import Role here (after PolicyRule is defined) to avoid circular imports
from osa.domain.auth.model.role import Role  # noqa: E402


class PolicySet:
    """Declarative set of all authorization rules.

    Evaluation: for a given action, rules are tried in order.
    First match wins (allow). No match means deny.
    """

    def __init__(self, rules: list[PolicyRule]) -> None:
        self._rules = rules
        self._by_action: dict[Action, list[PolicyRule]] = {}
        for rule in rules:
            self._by_action.setdefault(rule.action, []).append(rule)

    def guard(
        self,
        principal: "Principal | None",
        action: Action,
        resource: Any = None,
    ) -> None:
        """Raise AuthorizationError if no rule allows this access."""
        from osa.domain.shared.error import AuthorizationError

        principal_id = str(principal.user_id) if principal else "anonymous"

        rules = self._by_action.get(action, [])
        for rule in rules:
            if self._matches(rule, principal, resource):
                logger.info(
                    "Authorization allowed: principal=%s action=%s",
                    principal_id,
                    action,
                )
                return

        logger.warning(
            "Authorization denied: principal=%s action=%s",
            principal_id,
            action,
        )
        raise AuthorizationError(f"Access denied: {action}", code="access_denied")

    def _matches(
        self,
        rule: PolicyRule,
        principal: "Principal | None",
        resource: Any,
    ) -> bool:
        # Public rule (no role required)
        if rule.role is None:
            return True
        # Must be authenticated
        if principal is None:
            return False
        # Role hierarchy check
        if not principal.has_role(rule.role):
            return False
        # Relationship check (if required)
        if rule.relationship == Relationship.OWNER:
            owner_id = getattr(resource, "owner_id", None)
            if owner_id is None or owner_id != principal.user_id:
                return False
        return True

    def validate_coverage(self) -> None:
        """Startup check: every Action enum member must have at least one rule."""
        from osa.domain.shared.error import ConfigurationError

        covered = {r.action for r in self._rules}
        missing = set(Action) - covered
        if missing:
            raise ConfigurationError(f"Actions without policy rules: {missing}")


POLICY_SET = PolicySet(
    [
        # Public reads (no auth required)
        allow(Action.RECORD_READ),
        allow(Action.SEARCH_QUERY),
        allow(Action.SCHEMA_READ),
        allow(Action.TRAIT_READ),
        allow(Action.CONVENTION_READ),
        allow(Action.VOCABULARY_READ),
        allow(Action.VALIDATION_READ),
        # Depositions (ownership-scoped)
        allow(Action.DEPOSITION_CREATE, role=Role.DEPOSITOR),
        allow(Action.DEPOSITION_READ, role=Role.DEPOSITOR, relationship=Relationship.OWNER),
        allow(Action.DEPOSITION_UPDATE, role=Role.DEPOSITOR, relationship=Relationship.OWNER),
        allow(Action.DEPOSITION_SUBMIT, role=Role.DEPOSITOR, relationship=Relationship.OWNER),
        allow(Action.DEPOSITION_DELETE, role=Role.DEPOSITOR, relationship=Relationship.OWNER),
        # Curators can read all depositions (no ownership required)
        allow(Action.DEPOSITION_READ, role=Role.CURATOR),
        allow(Action.DEPOSITION_APPROVE, role=Role.CURATOR),
        allow(Action.DEPOSITION_REJECT, role=Role.CURATOR),
        # Registry (admin-only writes)
        allow(Action.SCHEMA_CREATE, role=Role.ADMIN),
        allow(Action.SCHEMA_UPDATE, role=Role.ADMIN),
        allow(Action.SCHEMA_DELETE, role=Role.ADMIN),
        allow(Action.TRAIT_CREATE, role=Role.ADMIN),
        allow(Action.TRAIT_UPDATE, role=Role.ADMIN),
        allow(Action.TRAIT_DELETE, role=Role.ADMIN),
        allow(Action.CONVENTION_CREATE, role=Role.ADMIN),
        allow(Action.CONVENTION_UPDATE, role=Role.ADMIN),
        allow(Action.CONVENTION_DELETE, role=Role.ADMIN),
        allow(Action.VOCABULARY_CREATE, role=Role.ADMIN),
        allow(Action.VOCABULARY_UPDATE, role=Role.ADMIN),
        allow(Action.VOCABULARY_DELETE, role=Role.ADMIN),
        # Validation
        allow(Action.VALIDATION_CREATE, role=Role.DEPOSITOR),
        # Administration (superadmin-only)
        allow(Action.ROLE_ASSIGN, role=Role.SUPERADMIN),
        allow(Action.ROLE_REVOKE, role=Role.SUPERADMIN),
        allow(Action.ROLE_READ, role=Role.SUPERADMIN),
    ]
)
