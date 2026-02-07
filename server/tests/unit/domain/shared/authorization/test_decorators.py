"""Tests for @reads/@writes repo decorators."""

import pytest

from osa.domain.auth.model.identity import Anonymous, System
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.decorators import reads, writes
from osa.domain.shared.authorization.resource import has_role, owner
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


class _FakeRepo:
    """Minimal repo with _identity and decorated methods."""

    def __init__(self, identity, resource=None):
        self._identity = identity
        self._resource = resource

    @reads(owner() | has_role(Role.CURATOR))
    async def get(self, key: str):
        return self._resource

    @writes(owner())
    async def save(self, resource) -> None:
        self._resource = resource


class TestReadsDecorator:
    @pytest.mark.asyncio
    async def test_reads_allows_owner(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        resource = _FakeResource(owner_id=user_id)
        repo = _FakeRepo(identity=principal, resource=resource)

        result = await repo.get("key")
        assert result is resource

    @pytest.mark.asyncio
    async def test_reads_allows_curator(self) -> None:
        curator = _make_principal(frozenset({Role.CURATOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=curator, resource=resource)

        result = await repo.get("key")
        assert result is resource

    @pytest.mark.asyncio
    async def test_reads_denies_non_owner_depositor(self) -> None:
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=depositor, resource=resource)

        with pytest.raises(AuthorizationError):
            await repo.get("key")

    @pytest.mark.asyncio
    async def test_reads_skips_check_when_none(self) -> None:
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        repo = _FakeRepo(identity=depositor, resource=None)

        result = await repo.get("key")
        assert result is None

    @pytest.mark.asyncio
    async def test_reads_allows_system(self) -> None:
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=System(), resource=resource)

        result = await repo.get("key")
        assert result is resource

    @pytest.mark.asyncio
    async def test_reads_denies_anonymous(self) -> None:
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=Anonymous(), resource=resource)

        with pytest.raises(AuthorizationError, match="Authentication required"):
            await repo.get("key")

    @pytest.mark.asyncio
    async def test_reads_denies_anonymous_with_missing_token_code(self) -> None:
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=Anonymous(), resource=resource)

        with pytest.raises(AuthorizationError) as exc_info:
            await repo.get("key")
        assert exc_info.value.code == "missing_token"

    @pytest.mark.asyncio
    async def test_reads_allows_admin_via_role_hierarchy(self) -> None:
        admin = _make_principal(frozenset({Role.ADMIN}))
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=admin, resource=resource)

        result = await repo.get("key")
        assert result is resource


class TestWritesDecorator:
    @pytest.mark.asyncio
    async def test_writes_allows_owner(self) -> None:
        user_id = UserId.generate()
        principal = _make_principal(frozenset({Role.DEPOSITOR}), user_id=user_id)
        resource = _FakeResource(owner_id=user_id)
        repo = _FakeRepo(identity=principal)

        await repo.save(resource)
        assert repo._resource is resource

    @pytest.mark.asyncio
    async def test_writes_denies_non_owner(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=principal)

        with pytest.raises(AuthorizationError):
            await repo.save(resource)

    @pytest.mark.asyncio
    async def test_writes_checks_before_execution(self) -> None:
        """Write check happens before the method body runs."""
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=principal)

        with pytest.raises(AuthorizationError):
            await repo.save(resource)

        # Method body never executed — _resource still None
        assert repo._resource is None

    @pytest.mark.asyncio
    async def test_writes_allows_system(self) -> None:
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=System())

        await repo.save(resource)
        assert repo._resource is resource

    @pytest.mark.asyncio
    async def test_writes_denies_anonymous(self) -> None:
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=Anonymous())

        with pytest.raises(AuthorizationError, match="Authentication required"):
            await repo.save(resource)

    @pytest.mark.asyncio
    async def test_writes_denies_anonymous_with_missing_token_code(self) -> None:
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=Anonymous())

        with pytest.raises(AuthorizationError) as exc_info:
            await repo.save(resource)
        assert exc_info.value.code == "missing_token"

    @pytest.mark.asyncio
    async def test_writes_denies_non_owner_with_access_denied_code(self) -> None:
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())
        repo = _FakeRepo(identity=principal)

        with pytest.raises(AuthorizationError) as exc_info:
            await repo.save(resource)
        assert exc_info.value.code == "access_denied"
