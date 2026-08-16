"""The auth gate behaves identically on the write and read sides (#F2, 2026-08-16).

The wrapper and metaclass live once in ``shared/handler.py``; these tests pin
the behavioral contract for BOTH facades so the gate semantics can never fork
between CommandHandler and QueryHandler again (they had drifted once: the query
copy debug-logged, the command copy didn't).
"""

from uuid import uuid4

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.gate import at_least, public, requires_scope
from osa.domain.shared.command import Command, CommandHandler
from osa.domain.shared.command import Result as CommandResult
from osa.domain.shared.error import AuthorizationError, ConfigurationError
from osa.domain.shared.query import Query, QueryHandler
from osa.domain.shared.query import Result as QueryResult


class Ping(Command):
    pass


class PingQ(Query):
    pass


class Pong(CommandResult):
    ok: bool = True


def _principal(
    *, roles: frozenset[Role] = frozenset(), scopes: frozenset[str] = frozenset()
) -> Principal:
    return Principal(
        user_id=UserId(uuid4()),
        provider_identity=ProviderIdentity(provider="test", external_id="u1"),
        roles=roles,
        scopes=scopes,
    )


def _make_pair(gate, *, with_principal: bool):
    """One CommandHandler and one QueryHandler with the same gate + body."""

    if with_principal:

        class Cmd(CommandHandler[Ping, Pong]):
            __auth__ = gate
            principal: Principal | None

            async def run(self, cmd: Ping) -> Pong:
                return Pong()

        class Qry(QueryHandler[PingQ, Pong]):
            __auth__ = gate
            principal: Principal | None

            async def run(self, cmd: PingQ) -> Pong:
                return Pong()

        return Cmd, Qry

    class CmdNoP(CommandHandler[Ping, Pong]):
        __auth__ = gate

        async def run(self, cmd: Ping) -> Pong:
            return Pong()

    class QryNoP(QueryHandler[PingQ, Pong]):
        __auth__ = gate

        async def run(self, cmd: PingQ) -> Pong:
            return Pong()

    return CmdNoP, QryNoP


@pytest.mark.asyncio
class TestGateParity:
    async def test_public_runs_without_principal_on_both(self):
        cmd_cls, qry_cls = _make_pair(public(), with_principal=False)
        assert (await cmd_cls().run(Ping())).ok
        assert (await qry_cls().run(PingQ())).ok

    async def test_at_least_missing_token_on_both(self):
        cmd_cls, qry_cls = _make_pair(at_least(Role.ADMIN), with_principal=True)
        for handler, dto in ((cmd_cls(principal=None), Ping()), (qry_cls(principal=None), PingQ())):
            with pytest.raises(AuthorizationError) as exc:
                await handler.run(dto)
            assert exc.value.code == "missing_token"

    async def test_at_least_access_denied_on_both(self):
        cmd_cls, qry_cls = _make_pair(at_least(Role.ADMIN), with_principal=True)
        weak = _principal(roles=frozenset({Role.DEPOSITOR}))
        for handler, dto in ((cmd_cls(principal=weak), Ping()), (qry_cls(principal=weak), PingQ())):
            with pytest.raises(AuthorizationError) as exc:
                await handler.run(dto)
            assert exc.value.code == "access_denied"

    async def test_at_least_admits_sufficient_role_on_both(self):
        cmd_cls, qry_cls = _make_pair(at_least(Role.ADMIN), with_principal=True)
        admin = _principal(roles=frozenset({Role.ADMIN}))
        assert (await cmd_cls(principal=admin).run(Ping())).ok
        assert (await qry_cls(principal=admin).run(PingQ())).ok

    async def test_requires_scope_admits_scope_or_admin_on_both(self):
        gate = requires_scope("things:write")
        cmd_cls, qry_cls = _make_pair(gate, with_principal=True)
        scoped = _principal(scopes=frozenset({"things:write"}))
        admin = _principal(roles=frozenset({Role.ADMIN}))
        unscoped = _principal(scopes=frozenset({"other:read"}))

        assert (await cmd_cls(principal=scoped).run(Ping())).ok
        assert (await qry_cls(principal=admin).run(PingQ())).ok
        for handler, dto in (
            (cmd_cls(principal=unscoped), Ping()),
            (qry_cls(principal=unscoped), PingQ()),
        ):
            with pytest.raises(AuthorizationError) as exc:
                await handler.run(dto)
            assert exc.value.code == "access_denied"

    async def test_missing_gate_is_a_configuration_error_on_both(self):
        class NoGateCmd(CommandHandler[Ping, Pong]):
            async def run(self, cmd: Ping) -> Pong:
                return Pong()

        class NoGateQry(QueryHandler[PingQ, Pong]):
            async def run(self, cmd: PingQ) -> Pong:
                return Pong()

        with pytest.raises(ConfigurationError):
            await NoGateCmd().run(Ping())
        with pytest.raises(ConfigurationError):
            await NoGateQry().run(PingQ())


def test_result_base_is_shared():
    """One Result class, re-exported by both facades — no forked DTO bases."""
    assert CommandResult is QueryResult
