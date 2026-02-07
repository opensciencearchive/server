"""Tests for Role hierarchy: T012 — numeric hierarchy comparison."""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.authorization.resource import has_role
from osa.domain.shared.command import Command, CommandHandler, Result


class TestRoleHierarchy:
    def test_admin_ge_curator(self) -> None:
        assert Role.ADMIN >= Role.CURATOR

    def test_depositor_lt_admin(self) -> None:
        assert not (Role.DEPOSITOR >= Role.ADMIN)

    def test_superadmin_gt_all(self) -> None:
        for role in Role:
            if role != Role.SUPERADMIN:
                assert Role.SUPERADMIN > role

    def test_public_is_lowest(self) -> None:
        for role in Role:
            assert role >= Role.PUBLIC

    def test_ordering(self) -> None:
        assert Role.PUBLIC < Role.DEPOSITOR < Role.CURATOR < Role.ADMIN < Role.SUPERADMIN


class TestPrincipalHasRole:
    def test_has_role_uses_hierarchy(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            provider_identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.ADMIN}),
        )

        # Admin >= Curator, so has_role(CURATOR) should be True
        assert principal.has_role(Role.CURATOR) is True
        assert principal.has_role(Role.ADMIN) is True
        assert principal.has_role(Role.SUPERADMIN) is False

    def test_has_role_depositor(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            provider_identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.DEPOSITOR}),
        )

        assert principal.has_role(Role.DEPOSITOR) is True
        assert principal.has_role(Role.CURATOR) is False
        assert principal.has_role(Role.ADMIN) is False

    def test_has_any_role(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            provider_identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.CURATOR}),
        )

        assert principal.has_any_role(Role.ADMIN, Role.CURATOR) is True
        assert principal.has_any_role(Role.SUPERADMIN) is False


def _make_principal(
    roles: frozenset[Role],
    user_id: UserId | None = None,
) -> Principal:
    return Principal(
        user_id=user_id or UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=roles,
    )


# --- Inline handler for gate test ---


class _MultiRoleCommand(Command):
    value: str = "test"


class _MultiRoleResult(Result):
    value: str


class _CuratorGatedHandler(CommandHandler[_MultiRoleCommand, _MultiRoleResult]):
    __auth__ = at_least(Role.CURATOR)
    principal: Principal

    async def run(self, cmd: _MultiRoleCommand) -> _MultiRoleResult:
        return _MultiRoleResult(value=cmd.value)


class _FakeResource:
    def __init__(self, owner_id: UserId) -> None:
        self.owner_id = owner_id


class TestMultiRolePrincipal:
    def test_multi_role_principal_has_role_uses_highest(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR, Role.CURATOR}))

        assert principal.has_role(Role.CURATOR) is True
        assert principal.has_role(Role.DEPOSITOR) is True

    def test_multi_role_principal_fails_above_highest(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR, Role.CURATOR}))

        assert principal.has_role(Role.ADMIN) is False

    @pytest.mark.asyncio
    async def test_multi_role_principal_at_handler_gate(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR, Role.CURATOR}))
        handler = _CuratorGatedHandler(principal=principal)

        result = await handler.run(_MultiRoleCommand(value="ok"))
        assert result.value == "ok"

    def test_multi_role_principal_at_resource_check(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR, Role.CURATOR}))
        check = has_role(Role.CURATOR)
        resource = _FakeResource(owner_id=UserId.generate())

        # Should not raise — CURATOR satisfies has_role(CURATOR)
        check.evaluate(principal, resource)
