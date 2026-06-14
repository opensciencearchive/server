"""Unit tests for the RequiresScope gate + Principal scopes (#145, US5, T052)."""

from __future__ import annotations

import pytest

from osa.domain.auth.model.identity import Anonymous
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.gate import RequiresScope, requires_scope
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import AuthorizationError


class _Cmd(Command):
    value: str = "x"


class _Res(Result):
    ok: bool = True


class _ScopedHandler(CommandHandler[_Cmd, _Res]):
    __auth__ = requires_scope("hooks:write")
    principal: Principal

    async def run(self, cmd: _Cmd) -> _Res:
        return _Res()


def _principal(
    *, roles: frozenset[Role] = frozenset(), scopes: frozenset[str] = frozenset()
) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="m2m", external_id="client-1"),
        roles=roles,
        scopes=scopes,
    )


def test_requires_scope_factory() -> None:
    gate = requires_scope("hooks:write")
    assert isinstance(gate, RequiresScope)
    assert gate.scope == "hooks:write"


def test_principal_has_scope() -> None:
    p = _principal(scopes=frozenset({"hooks:write"}))
    assert p.has_scope("hooks:write") is True
    assert p.has_scope("conventions:write") is False


class TestRequiresScopeGate:
    @pytest.mark.asyncio
    async def test_allows_matching_scope(self) -> None:
        handler = _ScopedHandler(principal=_principal(scopes=frozenset({"hooks:write"})))
        result = await handler.run(_Cmd())
        assert result.ok is True

    @pytest.mark.asyncio
    async def test_allows_admin_without_scope(self) -> None:
        handler = _ScopedHandler(principal=_principal(roles=frozenset({Role.ADMIN})))
        result = await handler.run(_Cmd())
        assert result.ok is True

    @pytest.mark.asyncio
    async def test_denies_wrong_scope_non_admin(self) -> None:
        handler = _ScopedHandler(
            principal=_principal(
                roles=frozenset({Role.DEPOSITOR}), scopes=frozenset({"other:write"})
            )
        )
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(_Cmd())
        assert exc.value.code == "access_denied"

    @pytest.mark.asyncio
    async def test_missing_principal_is_unauthenticated(self) -> None:
        handler = _ScopedHandler(principal=Anonymous())  # type: ignore[arg-type]
        with pytest.raises(AuthorizationError) as exc:
            await handler.run(_Cmd())
        assert exc.value.code == "missing_token"
