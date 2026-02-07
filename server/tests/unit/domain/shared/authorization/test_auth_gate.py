"""Tests for handler __auth__ gate: T013 — metaclass wraps run() with auth check."""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.policy import requires_role
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import AuthorizationError, ConfigurationError
from osa.domain.shared.query import Query, QueryHandler
from osa.domain.shared.query import Result as QueryResult


def _make_principal(roles: frozenset[Role]) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        identity=ProviderIdentity(provider="test", external_id="test-ext"),
        roles=roles,
    )


# --- Test command DTOs ---


class AdminOnlyCommand(Command):
    value: str = "test"


class AdminOnlyResult(Result):
    value: str


class PublicCommand(Command):
    __public__: bool = True  # ClassVar-like, signals public access
    value: str = "test"


class PublicResult(Result):
    value: str


# --- Test handlers ---


class AdminOnlyHandler(CommandHandler[AdminOnlyCommand, AdminOnlyResult]):
    __auth__ = requires_role(Role.ADMIN)
    _principal: Principal | None = None

    async def run(self, cmd: AdminOnlyCommand) -> AdminOnlyResult:
        return AdminOnlyResult(value=cmd.value)


class PublicHandler(CommandHandler[PublicCommand, PublicResult]):
    _principal: Principal | None = None

    async def run(self, cmd: PublicCommand) -> PublicResult:
        return PublicResult(value=cmd.value)


class UnprotectedCommand(Command):
    value: str = "test"


class UnprotectedResult(Result):
    value: str


class UnprotectedHandler(CommandHandler[UnprotectedCommand, UnprotectedResult]):
    async def run(self, cmd: UnprotectedCommand) -> UnprotectedResult:
        return UnprotectedResult(value=cmd.value)


class UnprotectedQuery(Query):
    value: str = "test"


class UnprotectedQueryResult(QueryResult):
    value: str


class UnprotectedQueryHandler(QueryHandler[UnprotectedQuery, UnprotectedQueryResult]):
    async def run(self, cmd: UnprotectedQuery) -> UnprotectedQueryResult:
        return UnprotectedQueryResult(value=cmd.value)


# --- Tests ---


class TestAuthGateOnCommandHandler:
    @pytest.mark.asyncio
    async def test_admin_handler_rejects_depositor(self) -> None:
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        handler = AdminOnlyHandler(_principal=depositor)

        with pytest.raises(AuthorizationError):
            await handler.run(AdminOnlyCommand(value="test"))

    @pytest.mark.asyncio
    async def test_admin_handler_allows_admin(self) -> None:
        admin = _make_principal(frozenset({Role.ADMIN}))
        handler = AdminOnlyHandler(_principal=admin)

        result = await handler.run(AdminOnlyCommand(value="hello"))
        assert result.value == "hello"

    @pytest.mark.asyncio
    async def test_admin_handler_rejects_none_principal(self) -> None:
        handler = AdminOnlyHandler(_principal=None)

        with pytest.raises(AuthorizationError):
            await handler.run(AdminOnlyCommand(value="test"))

    @pytest.mark.asyncio
    async def test_public_handler_skips_check(self) -> None:
        handler = PublicHandler(_principal=None)

        result = await handler.run(PublicCommand(value="public"))
        assert result.value == "public"

    @pytest.mark.asyncio
    async def test_public_handler_works_with_principal(self) -> None:
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        handler = PublicHandler(_principal=depositor)

        result = await handler.run(PublicCommand(value="public"))
        assert result.value == "public"

    @pytest.mark.asyncio
    async def test_unprotected_command_handler_raises_configuration_error(self) -> None:
        handler = UnprotectedHandler()

        with pytest.raises(ConfigurationError, match="UnprotectedHandler"):
            await handler.run(UnprotectedCommand(value="test"))

    @pytest.mark.asyncio
    async def test_unprotected_query_handler_raises_configuration_error(self) -> None:
        handler = UnprotectedQueryHandler()

        with pytest.raises(ConfigurationError, match="UnprotectedQueryHandler"):
            await handler.run(UnprotectedQuery(value="test"))
