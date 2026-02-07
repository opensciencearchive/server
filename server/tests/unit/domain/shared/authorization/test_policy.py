"""Tests for Policy composition: T011 — composable handler-level policies."""

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.policy import (
    AllOf,
    RequiresRole,
    requires_any_role,
    requires_role,
)


def _make_principal(roles: frozenset[Role]) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        identity=ProviderIdentity(provider="test", external_id="test-ext"),
        roles=roles,
    )


class TestRequiresRole:
    def test_admin_policy_denies_depositor(self) -> None:
        policy = requires_role(Role.ADMIN)
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))

        assert policy.evaluate(depositor) is False

    def test_admin_policy_allows_admin(self) -> None:
        policy = requires_role(Role.ADMIN)
        admin = _make_principal(frozenset({Role.ADMIN}))

        assert policy.evaluate(admin) is True

    def test_admin_policy_allows_superadmin(self) -> None:
        policy = requires_role(Role.ADMIN)
        superadmin = _make_principal(frozenset({Role.SUPERADMIN}))

        assert policy.evaluate(superadmin) is True


class TestPolicyComposition:
    def test_or_operator(self) -> None:
        policy = requires_role(Role.ADMIN) | requires_role(Role.CURATOR)
        curator = _make_principal(frozenset({Role.CURATOR}))

        assert policy.evaluate(curator) is True

    def test_and_operator(self) -> None:
        # Both must pass — depositor + curator would fail an AllOf(admin, curator)
        policy = requires_role(Role.ADMIN) & requires_role(Role.CURATOR)
        admin = _make_principal(frozenset({Role.ADMIN}))

        # Admin >= Curator, so both should pass via hierarchy
        assert policy.evaluate(admin) is True

    def test_not_operator(self) -> None:
        policy = ~requires_role(Role.ADMIN)
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))

        assert policy.evaluate(depositor) is True

    def test_not_inverts(self) -> None:
        policy = ~requires_role(Role.ADMIN)
        admin = _make_principal(frozenset({Role.ADMIN}))

        assert policy.evaluate(admin) is False


class TestAllOf:
    def test_all_must_pass(self) -> None:
        policy = AllOf(policies=(RequiresRole(Role.DEPOSITOR), RequiresRole(Role.CURATOR)))
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))

        # Depositor < Curator, so second check fails
        assert policy.evaluate(depositor) is False


class TestRequiresAnyRole:
    def test_any_role_works(self) -> None:
        policy = requires_any_role(Role.ADMIN, Role.CURATOR)
        curator = _make_principal(frozenset({Role.CURATOR}))

        assert policy.evaluate(curator) is True

    def test_any_role_fails_when_none_match(self) -> None:
        policy = requires_any_role(Role.ADMIN, Role.SUPERADMIN)
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))

        assert policy.evaluate(depositor) is False
