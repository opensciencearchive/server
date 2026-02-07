"""Tests for Identity hierarchy: Anonymous, System, Principal subclassing."""

from osa.domain.auth.model.identity import Anonymous, Identity, System
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId


class TestIdentityHierarchy:
    def test_anonymous_is_identity(self) -> None:
        anon = Anonymous()
        assert isinstance(anon, Identity)

    def test_system_is_identity(self) -> None:
        system = System()
        assert isinstance(system, Identity)

    def test_principal_is_identity(self) -> None:
        principal = Principal(
            user_id=UserId.generate(),
            provider_identity=ProviderIdentity(provider="test", external_id="ext"),
            roles=frozenset({Role.DEPOSITOR}),
        )
        assert isinstance(principal, Identity)

    def test_anonymous_is_not_principal(self) -> None:
        anon = Anonymous()
        assert not isinstance(anon, Principal)

    def test_system_is_not_principal(self) -> None:
        system = System()
        assert not isinstance(system, Principal)

    def test_anonymous_is_frozen(self) -> None:
        anon = Anonymous()
        assert hash(anon) is not None  # frozen dataclass is hashable

    def test_system_is_frozen(self) -> None:
        system = System()
        assert hash(system) is not None
