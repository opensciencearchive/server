"""Tests for ResourceCheck: evaluate() with System/Anonymous/Principal, OwnerCheck, HasRole, AnyOf."""

import pytest

from osa.domain.auth.model.identity import Anonymous, System
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.resource import (
    AnyOf,
    has_role,
    owner,
)
from osa.domain.shared.error import AuthorizationError


def _make_principal(
    roles: frozenset[Role],
    user_id: UserId | None = None,
) -> Principal:
    return Principal(
        user_id=user_id or UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=roles,
    )


class _FakeResource:
    def __init__(self, owner_id: UserId) -> None:
        self.owner_id = owner_id


class TestResourceCheckSystemBypass:
    def test_system_bypasses_owner_check(self) -> None:
        check = owner()
        resource = _FakeResource(owner_id=UserId.generate())
        # Should not raise
        check.evaluate(System(), resource)

    def test_system_bypasses_has_role_check(self) -> None:
        check = has_role(Role.SUPERADMIN)
        resource = _FakeResource(owner_id=UserId.generate())
        check.evaluate(System(), resource)

    def test_system_bypasses_any_of_check(self) -> None:
        check = owner() | has_role(Role.ADMIN)
        resource = _FakeResource(owner_id=UserId.generate())
        check.evaluate(System(), resource)


class TestResourceCheckAnonymousRejection:
    def test_anonymous_rejected_by_owner_check(self) -> None:
        check = owner()
        resource = _FakeResource(owner_id=UserId.generate())
        with pytest.raises(AuthorizationError, match="Authentication required"):
            check.evaluate(Anonymous(), resource)

    def test_anonymous_rejected_by_has_role_check(self) -> None:
        check = has_role(Role.DEPOSITOR)
        resource = _FakeResource(owner_id=UserId.generate())
        with pytest.raises(AuthorizationError, match="Authentication required"):
            check.evaluate(Anonymous(), resource)


class TestOwnerCheck:
    def test_owner_passes(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        resource = _FakeResource(owner_id=user_id)
        owner().evaluate(principal, resource)

    def test_non_owner_denied(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        with pytest.raises(AuthorizationError, match="not resource owner"):
            owner().evaluate(principal, resource)

    def test_resource_without_owner_id_denied(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))

        class NoOwner:
            pass

        with pytest.raises(AuthorizationError, match="not resource owner"):
            owner().evaluate(principal, NoOwner())


class TestHasRole:
    def test_principal_with_sufficient_role_passes(self) -> None:
        principal = _make_principal(frozenset({Role.ADMIN}))
        resource = _FakeResource(owner_id=UserId.generate())
        has_role(Role.CURATOR).evaluate(principal, resource)

    def test_principal_with_exact_role_passes(self) -> None:
        principal = _make_principal(frozenset({Role.CURATOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        has_role(Role.CURATOR).evaluate(principal, resource)

    def test_principal_with_insufficient_role_denied(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        with pytest.raises(AuthorizationError, match="requires role CURATOR"):
            has_role(Role.CURATOR).evaluate(principal, resource)


class TestAnyOf:
    def test_passes_when_first_check_passes(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        resource = _FakeResource(owner_id=user_id)

        check = owner() | has_role(Role.CURATOR)
        check.evaluate(principal, resource)

    def test_passes_when_second_check_passes(self) -> None:
        principal = _make_principal(frozenset({Role.CURATOR}))
        resource = _FakeResource(owner_id=UserId.generate())  # not owner

        check = owner() | has_role(Role.CURATOR)
        check.evaluate(principal, resource)

    def test_fails_when_no_check_passes(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())  # not owner, not curator

        check = owner() | has_role(Role.CURATOR)
        with pytest.raises(AuthorizationError, match="Access denied"):
            check.evaluate(principal, resource)


class TestOrOperator:
    def test_pipe_creates_any_of(self) -> None:
        check = owner() | has_role(Role.CURATOR)
        assert isinstance(check, AnyOf)

    def test_chained_pipe(self) -> None:
        check = owner() | has_role(Role.CURATOR) | has_role(Role.ADMIN)
        assert isinstance(check, AnyOf)
        assert len(check.checks) == 3
