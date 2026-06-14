"""Unit tests for the hook release / live-pointer command + query handlers (#145).

US3 (create release, catalog/history/detail) and US4 (set live). Services are
mocked; these assert the handler orchestration + auth gates only.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.gate import Public
from osa.domain.shared.error import AuthorizationError, NotFoundError
from osa.domain.shared.model.hook import ColumnDef, HookName, OciConfig, OciLimits, TableFeatureSpec
from osa.domain.validation.model.hook import Hook
from osa.domain.validation.model.hook_release import HookRelease, HookReleaseId, ReleaseOutcome

NAME = HookName("pocket_detect")


def _principal(role: Role = Role.ADMIN) -> Principal:
    return Principal(
        user_id=UserId.generate(),
        provider_identity=ProviderIdentity(provider="test", external_id="ext"),
        roles=frozenset({role}),
    )


def _feature() -> TableFeatureSpec:
    return TableFeatureSpec(
        cardinality="many",
        columns=[ColumnDef(name="score", json_type="number", required=True)],
    )


def _release(version: int, digest: str, rid: HookReleaseId | None = None) -> HookRelease:
    return HookRelease(
        id=rid or HookReleaseId(uuid4()),
        hook_name=NAME,
        version=version,
        runtime=OciConfig(image="reg/pocket:abc", digest=digest, config={}, limits=OciLimits()),
        source_ref="git-sha",
        built_by=None,
        built_at=datetime.now(UTC),
    )


def _hook(live: HookReleaseId | None) -> Hook:
    return Hook(name=NAME, feature=_feature(), live_release_id=live, created_at=datetime.now(UTC))


class TestCreateReleaseHandler:
    @pytest.mark.asyncio
    async def test_new_version_is_live_and_created(self) -> None:
        from osa.domain.validation.command.create_release import (
            CreateRelease,
            CreateReleaseHandler,
        )

        new = _release(2, "sha256:new")
        service = AsyncMock()
        service.create_release.return_value = ReleaseOutcome(release=new, created=True)
        service.get_hook.return_value = _hook(live=new.id)

        handler = CreateReleaseHandler(principal=_principal(), service=service)
        result = await handler.run(
            CreateRelease(name=NAME, image="reg/pocket:def", digest="sha256:new", source_ref="git")
        )

        assert result.version == 2
        assert result.live is True
        assert result.created is True

    @pytest.mark.asyncio
    async def test_idempotent_redeploy_is_not_created(self) -> None:
        from osa.domain.validation.command.create_release import (
            CreateRelease,
            CreateReleaseHandler,
        )

        existing = _release(1, "sha256:old")
        service = AsyncMock()
        service.create_release.return_value = ReleaseOutcome(release=existing, created=False)
        service.get_hook.return_value = _hook(live=existing.id)

        handler = CreateReleaseHandler(principal=_principal(), service=service)
        result = await handler.run(
            CreateRelease(name=NAME, image="reg/pocket:abc", digest="sha256:old", source_ref="git")
        )

        assert result.created is False
        assert result.version == 1

    @pytest.mark.asyncio
    async def test_requires_admin(self) -> None:
        from osa.domain.validation.command.create_release import (
            CreateRelease,
            CreateReleaseHandler,
        )

        handler = CreateReleaseHandler(principal=_principal(Role.DEPOSITOR), service=AsyncMock())
        with pytest.raises(AuthorizationError):
            await handler.run(CreateRelease(name=NAME, image="i", digest="d", source_ref="git"))


class TestSetLiveHandler:
    @pytest.mark.asyncio
    async def test_repoints_live(self) -> None:
        from osa.domain.validation.command.set_live import SetLive, SetLiveHandler

        target = _release(1, "sha256:old")
        service = AsyncMock()
        service.set_live.return_value = _hook(live=target.id)

        handler = SetLiveHandler(principal=_principal(), service=service)
        result = await handler.run(SetLive(name=NAME, version=1))

        assert result.live_version == 1
        assert result.live_release_id == target.id
        service.set_live.assert_awaited_once_with(NAME, 1)

    @pytest.mark.asyncio
    async def test_unknown_version_propagates(self) -> None:
        from osa.domain.validation.command.set_live import SetLive, SetLiveHandler

        service = AsyncMock()
        service.set_live.side_effect = NotFoundError("nope")

        handler = SetLiveHandler(principal=_principal(), service=service)
        with pytest.raises(NotFoundError):
            await handler.run(SetLive(name=NAME, version=99))


class TestListHooksHandler:
    @pytest.mark.asyncio
    async def test_catalog_includes_live_release(self) -> None:
        from osa.domain.validation.query.list_hooks import HookCatalog, ListHooks, ListHooksHandler

        live = _release(3, "sha256:live")
        service = AsyncMock()
        service.list_hooks.return_value = [_hook(live=live.id)]
        service.resolve_live.return_value = {NAME: live}

        handler = ListHooksHandler(service=service)
        result: HookCatalog = await handler.run(ListHooks())

        assert len(result.items) == 1
        assert result.items[0].live_release is not None
        assert result.items[0].live_release.version == 3

    @pytest.mark.asyncio
    async def test_hook_without_live_release(self) -> None:
        from osa.domain.validation.query.list_hooks import ListHooks, ListHooksHandler

        service = AsyncMock()
        service.list_hooks.return_value = [_hook(live=None)]
        service.resolve_live.return_value = {}

        handler = ListHooksHandler(service=service)
        result = await handler.run(ListHooks())

        assert result.items[0].live_release is None

    def test_is_public(self) -> None:
        from osa.domain.validation.query.list_hooks import ListHooksHandler

        assert isinstance(ListHooksHandler.__auth__, Public)


class TestListReleasesHandler:
    @pytest.mark.asyncio
    async def test_history_descending_with_live_version(self) -> None:
        from osa.domain.validation.query.list_releases import ListReleases, ListReleasesHandler

        v2 = _release(2, "sha256:v2")
        v1 = _release(1, "sha256:v1")
        service = AsyncMock()
        service.get_hook.return_value = _hook(live=v2.id)
        service.list_releases.return_value = [v2, v1]

        handler = ListReleasesHandler(service=service)
        result = await handler.run(ListReleases(name=NAME))

        assert [r.version for r in result.releases] == [2, 1]
        assert result.live_version == 2

    @pytest.mark.asyncio
    async def test_unknown_hook_404(self) -> None:
        from osa.domain.validation.query.list_releases import ListReleases, ListReleasesHandler

        service = AsyncMock()
        service.get_hook.return_value = None

        handler = ListReleasesHandler(service=service)
        with pytest.raises(NotFoundError):
            await handler.run(ListReleases(name=NAME))


class TestGetReleaseHandler:
    @pytest.mark.asyncio
    async def test_detail_marks_live(self) -> None:
        from osa.domain.validation.query.get_release import GetRelease, GetReleaseHandler

        rel = _release(2, "sha256:v2")
        service = AsyncMock()
        service.get_release.return_value = rel
        service.get_hook.return_value = _hook(live=rel.id)

        handler = GetReleaseHandler(service=service)
        result = await handler.run(GetRelease(name=NAME, version=2))

        assert result.version == 2
        assert result.live is True
        assert result.image == "reg/pocket:abc"

    @pytest.mark.asyncio
    async def test_unknown_release_404(self) -> None:
        from osa.domain.validation.query.get_release import GetRelease, GetReleaseHandler

        service = AsyncMock()
        service.get_release.return_value = None

        handler = GetReleaseHandler(service=service)
        with pytest.raises(NotFoundError):
            await handler.run(GetRelease(name=NAME, version=99))
