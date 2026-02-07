"""Tests for authorization error codes: pin 401 vs 403 mapping."""

import pytest

from osa.application.api.v1.errors import map_osa_error
from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.gate import at_least
from osa.domain.shared.authorization.resource import has_role, owner
from osa.domain.shared.command import Command, CommandHandler, Result
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


# --- Inline handler for gate-level tests ---


class _GatedCommand(Command):
    value: str = "test"


class _GatedResult(Result):
    value: str


class _AdminGatedHandler(CommandHandler[_GatedCommand, _GatedResult]):
    __auth__ = at_least(Role.ADMIN)
    principal: Principal

    async def run(self, cmd: _GatedCommand) -> _GatedResult:
        return _GatedResult(value=cmd.value)


# --- Handler gate error codes ---


class TestHandlerGateErrorCodes:
    @pytest.mark.asyncio
    async def test_missing_principal_has_missing_token_code(self) -> None:
        handler = _AdminGatedHandler.__new__(_AdminGatedHandler)

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(_GatedCommand(value="test"))
        assert exc_info.value.code == "missing_token"

    @pytest.mark.asyncio
    async def test_insufficient_role_has_access_denied_code(self) -> None:
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        handler = _AdminGatedHandler(principal=depositor)

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(_GatedCommand(value="test"))
        assert exc_info.value.code == "access_denied"


# --- Resource check error codes ---


class _FakeResource:
    def __init__(self, owner_id: UserId) -> None:
        self.owner_id = owner_id


class TestResourceCheckErrorCodes:
    def test_anonymous_resource_check_has_missing_token_code(self) -> None:
        check = has_role(Role.CURATOR)
        resource = _FakeResource(owner_id=UserId.generate())

        with pytest.raises(AuthorizationError) as exc_info:
            check.evaluate(Anonymous(), resource)
        assert exc_info.value.code == "missing_token"

    def test_owner_check_failure_has_access_denied_code(self) -> None:
        check = owner()
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())

        with pytest.raises(AuthorizationError) as exc_info:
            check.evaluate(principal, resource)
        assert exc_info.value.code == "access_denied"

    def test_has_role_check_failure_has_access_denied_code(self) -> None:
        check = has_role(Role.ADMIN)
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())

        with pytest.raises(AuthorizationError) as exc_info:
            check.evaluate(principal, resource)
        assert exc_info.value.code == "access_denied"

    def test_any_of_failure_has_access_denied_code(self) -> None:
        check = owner() | has_role(Role.ADMIN)
        principal = _make_principal(frozenset({Role.DEPOSITOR}))
        resource = _FakeResource(owner_id=UserId.generate())

        with pytest.raises(AuthorizationError) as exc_info:
            check.evaluate(principal, resource)
        assert exc_info.value.code == "access_denied"


# --- HTTP status code mapping ---


class TestErrorCodeToHttpMapping:
    def test_map_osa_error_missing_token_returns_401(self) -> None:
        error = AuthorizationError("Authentication required", code="missing_token")
        http_exc = map_osa_error(error)
        assert http_exc.status_code == 401

    def test_map_osa_error_access_denied_returns_403(self) -> None:
        error = AuthorizationError("Access denied", code="access_denied")
        http_exc = map_osa_error(error)
        assert http_exc.status_code == 403
