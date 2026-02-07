"""Tests for Guarded[T]: T009 — generic authorization wrapper."""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.action import Action
from osa.domain.shared.authorization.guarded import Guarded
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


class _FakeDeposition:
    def __init__(self, owner_id: UserId, status: str = "draft") -> None:
        self.owner_id = owner_id
        self.status = status


class TestGuardedCheck:
    def test_check_returns_unwrapped_resource_on_success(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        dep = _FakeDeposition(owner_id=user_id)

        guarded = Guarded(dep, principal, POLICY_SET)
        result = guarded.check(Action.DEPOSITION_READ)

        assert result is dep
        assert result.status == "draft"

    def test_check_raises_on_failure(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        dep = _FakeDeposition(owner_id=UserId.generate())  # different owner

        guarded = Guarded(dep, principal, POLICY_SET)

        with pytest.raises(AuthorizationError):
            guarded.check(Action.DEPOSITION_SUBMIT)


class TestGuardedNoProxy:
    def test_no_attribute_access_proxy(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        dep = _FakeDeposition(owner_id=user_id)

        guarded = Guarded(dep, principal, POLICY_SET)

        # Accessing .status on Guarded[Deposition] should raise AttributeError
        with pytest.raises(AttributeError):
            _ = guarded.status  # type: ignore[attr-defined]
