"""Tests for PolicySet: T007 — declarative authorization rules."""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.action import Action
from osa.domain.shared.authorization.policy_set import POLICY_SET
from osa.domain.shared.error import AuthorizationError


def _make_principal(
    roles: frozenset[Role],
    user_id: UserId | None = None,
) -> Principal:
    uid = user_id or UserId.generate()
    return Principal(
        user_id=uid,
        identity=ProviderIdentity(provider="test", external_id="test-ext"),
        roles=roles,
    )


class _FakeResource:
    """Fake resource with owner_id for testing ownership checks."""

    def __init__(self, owner_id: UserId) -> None:
        self.owner_id = owner_id


class TestPolicySetOwnership:
    def test_owner_can_submit_own_deposition(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        resource = _FakeResource(owner_id=user_id)

        # Should not raise
        POLICY_SET.guard(principal, Action.DEPOSITION_SUBMIT, resource)

    def test_non_owner_denied_submit(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())  # different user

        with pytest.raises(AuthorizationError):
            POLICY_SET.guard(principal, Action.DEPOSITION_SUBMIT, resource)

    def test_owner_can_read_own_deposition(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        resource = _FakeResource(owner_id=user_id)

        POLICY_SET.guard(principal, Action.DEPOSITION_READ, resource)

    def test_non_owner_depositor_denied_read(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())

        with pytest.raises(AuthorizationError):
            POLICY_SET.guard(principal, Action.DEPOSITION_READ, resource)


class TestPolicySetRoles:
    def test_admin_reads_any_deposition(self) -> None:
        principal = _make_principal(frozenset({Role.ADMIN}))
        resource = _FakeResource(owner_id=UserId.generate())

        # Admin >= Curator, so curator read rule (no ownership) should match
        POLICY_SET.guard(principal, Action.DEPOSITION_READ, resource)

    def test_curator_reads_any_deposition(self) -> None:
        principal = _make_principal(frozenset({Role.CURATOR}))
        resource = _FakeResource(owner_id=UserId.generate())

        POLICY_SET.guard(principal, Action.DEPOSITION_READ, resource)

    def test_depositor_cannot_create_schema(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))

        with pytest.raises(AuthorizationError):
            POLICY_SET.guard(principal, Action.SCHEMA_CREATE)

    def test_admin_can_create_schema(self) -> None:
        principal = _make_principal(frozenset({Role.ADMIN}))

        POLICY_SET.guard(principal, Action.SCHEMA_CREATE)


class TestPolicySetPublic:
    def test_public_user_reads_records(self) -> None:
        # No principal (anonymous)
        POLICY_SET.guard(None, Action.RECORD_READ)

    def test_public_user_can_search(self) -> None:
        POLICY_SET.guard(None, Action.SEARCH_QUERY)

    def test_public_user_reads_schemas(self) -> None:
        POLICY_SET.guard(None, Action.SCHEMA_READ)


class TestPolicySetCoverage:
    def test_validate_coverage_passes(self) -> None:
        # Should not raise — all actions should be covered
        POLICY_SET.validate_coverage()

    def test_validate_coverage_catches_missing(self) -> None:
        from osa.domain.shared.authorization.policy_set import PolicySet, allow
        from osa.domain.shared.error import ConfigurationError

        # Incomplete policy set
        incomplete = PolicySet([allow(Action.RECORD_READ)])
        with pytest.raises(ConfigurationError):
            incomplete.validate_coverage()
