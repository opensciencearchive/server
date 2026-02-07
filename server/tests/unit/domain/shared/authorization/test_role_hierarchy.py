"""Tests for Role hierarchy: T012 — numeric hierarchy comparison."""

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId


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
            identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.ADMIN}),
        )

        # Admin >= Curator, so has_role(CURATOR) should be True
        assert principal.has_role(Role.CURATOR) is True
        assert principal.has_role(Role.ADMIN) is True
        assert principal.has_role(Role.SUPERADMIN) is False

    def test_has_role_depositor(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.DEPOSITOR}),
        )

        assert principal.has_role(Role.DEPOSITOR) is True
        assert principal.has_role(Role.CURATOR) is False
        assert principal.has_role(Role.ADMIN) is False

    def test_has_any_role(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.CURATOR}),
        )

        assert principal.has_any_role(Role.ADMIN, Role.CURATOR) is True
        assert principal.has_any_role(Role.SUPERADMIN) is False
