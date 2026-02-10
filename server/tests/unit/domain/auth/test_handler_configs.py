"""Tests for concrete handler auth configurations.

Verifies that production handlers enforce their declared __auth__ gates
end-to-end (real handler classes, mocked services).
"""

from unittest.mock import AsyncMock, MagicMock

import pytest

from osa.domain.auth.command.assign_role import (
    AssignRole,
    AssignRoleHandler,
)
from osa.domain.auth.command.login import (
    InitiateLogin,
    InitiateLoginHandler,
)
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.auth.service.token import TokenService
from osa.domain.deposition.command.create import (
    CreateDeposition,
    CreateDepositionHandler,
)
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


class TestCreateDepositionHandlerAuth:
    @pytest.mark.asyncio
    async def test_create_deposition_allows_depositor(self) -> None:
        from osa.domain.shared.model.srn import ConventionSRN, DepositionSRN

        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        service = AsyncMock()
        mock_dep = MagicMock()
        mock_dep.srn = DepositionSRN.parse("urn:osa:localhost:dep:test-dep")
        service.create.return_value = mock_dep
        handler = CreateDepositionHandler(
            principal=depositor,
            deposition_service=service,
        )

        conv_srn = ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")
        result = await handler.run(CreateDeposition(convention_srn=conv_srn))
        assert result.srn is not None

    @pytest.mark.asyncio
    async def test_create_deposition_rejects_unauthenticated(self) -> None:
        from osa.domain.shared.model.srn import ConventionSRN

        handler = CreateDepositionHandler.__new__(CreateDepositionHandler)

        with pytest.raises(AuthorizationError) as exc_info:
            conv_srn = ConventionSRN.parse("urn:osa:localhost:conv:test@1.0.0")
            await handler.run(CreateDeposition(convention_srn=conv_srn))
        assert exc_info.value.code == "missing_token"


class TestAssignRoleHandlerAuth:
    @pytest.mark.asyncio
    async def test_assign_role_allows_superadmin(self) -> None:
        superadmin = _make_principal(frozenset({Role.SUPERADMIN}))
        service = AsyncMock()
        # Mock the return value to match what the handler expects
        from datetime import UTC, datetime

        from osa.domain.auth.model.role_assignment import RoleAssignment, RoleAssignmentId

        target_user_id = UserId.generate()
        service.assign_role.return_value = RoleAssignment(
            id=RoleAssignmentId.generate(),
            user_id=target_user_id,
            role=Role.CURATOR,
            assigned_by=superadmin.user_id,
            assigned_at=datetime.now(UTC),
        )

        handler = AssignRoleHandler(
            principal=superadmin,
            authorization_service=service,
        )

        result = await handler.run(AssignRole(user_id=str(target_user_id), role="curator"))
        assert result.role == "curator"

    @pytest.mark.asyncio
    async def test_assign_role_rejects_admin(self) -> None:
        admin = _make_principal(frozenset({Role.ADMIN}))
        service = AsyncMock()
        handler = AssignRoleHandler(
            principal=admin,
            authorization_service=service,
        )

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(AssignRole(user_id=str(UserId.generate()), role="curator"))
        assert exc_info.value.code == "access_denied"


class TestListDepositionsHandlerAuth:
    @pytest.mark.asyncio
    async def test_list_depositions_allows_depositor(self) -> None:
        from osa.domain.deposition.query.list_depositions import (
            DepositionList,
            ListDepositions,
            ListDepositionsHandler,
        )

        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        service = AsyncMock()
        service.list_depositions.return_value = ([], 0)
        handler = ListDepositionsHandler(
            principal=depositor,
            deposition_service=service,
        )

        result = await handler.run(ListDepositions())
        assert isinstance(result, DepositionList)
        assert result.items == []
        assert result.total == 0
        # Depositor sees own depositions only
        service.list_depositions.assert_called_once_with(depositor.user_id)

    @pytest.mark.asyncio
    async def test_list_depositions_curator_sees_all(self) -> None:
        from osa.domain.deposition.query.list_depositions import (
            DepositionList,
            ListDepositions,
            ListDepositionsHandler,
        )

        curator = _make_principal(frozenset({Role.CURATOR}))
        service = AsyncMock()
        service.list_depositions.return_value = ([], 0)
        handler = ListDepositionsHandler(
            principal=curator,
            deposition_service=service,
        )

        result = await handler.run(ListDepositions())
        assert isinstance(result, DepositionList)
        # Curator sees all depositions (owner_id=None)
        service.list_depositions.assert_called_once_with(None)

    @pytest.mark.asyncio
    async def test_list_depositions_rejects_unauthenticated(self) -> None:
        from osa.domain.deposition.query.list_depositions import (
            ListDepositions,
            ListDepositionsHandler,
        )

        handler = ListDepositionsHandler.__new__(ListDepositionsHandler)

        with pytest.raises(AuthorizationError) as exc_info:
            await handler.run(ListDepositions())
        assert exc_info.value.code == "missing_token"


class TestDownloadFileHandlerAuth:
    @pytest.mark.asyncio
    async def test_download_file_allows_depositor(self) -> None:
        from datetime import UTC, datetime

        from osa.domain.deposition.model.value import DepositionFile
        from osa.domain.deposition.query.download_file import (
            DownloadFile,
            DownloadFileHandler,
        )
        from osa.domain.shared.model.srn import DepositionSRN

        depositor = _make_principal(frozenset({Role.DEPOSITOR}))
        service = AsyncMock()
        file_meta = DepositionFile(
            name="data.csv", size=100, checksum="abc", uploaded_at=datetime.now(UTC)
        )

        async def _fake_stream():
            yield b"data"

        service.get_file_download.return_value = (_fake_stream(), file_meta)

        handler = DownloadFileHandler(
            principal=depositor,
            deposition_service=service,
        )

        srn = DepositionSRN.parse("urn:osa:localhost:dep:test-dep")
        result = await handler.run(DownloadFile(srn=srn, filename="data.csv"))
        assert result.filename == "data.csv"
        assert result.size == 100

    @pytest.mark.asyncio
    async def test_download_file_rejects_unauthenticated(self) -> None:
        from osa.domain.deposition.query.download_file import (
            DownloadFile,
            DownloadFileHandler,
        )
        from osa.domain.shared.model.srn import DepositionSRN

        handler = DownloadFileHandler.__new__(DownloadFileHandler)

        with pytest.raises(AuthorizationError) as exc_info:
            srn = DepositionSRN.parse("urn:osa:localhost:dep:test-dep")
            await handler.run(DownloadFile(srn=srn, filename="data.csv"))
        assert exc_info.value.code == "missing_token"


class TestInitiateLoginHandlerAuth:
    @pytest.mark.asyncio
    async def test_public_login_handler_works_without_principal(self) -> None:
        provider_registry = MagicMock()
        identity_provider = MagicMock()
        identity_provider.get_authorization_url.return_value = "https://example.com/auth"
        provider_registry.get.return_value = identity_provider

        from osa.config import JwtConfig

        token_service = TokenService(
            _config=JwtConfig(
                secret="test-secret-key-256-bits-long-xx",
                algorithm="HS256",
                access_token_expire_minutes=60,
                refresh_token_expire_days=7,
            )
        )

        handler = InitiateLoginHandler(
            provider_registry=provider_registry,
            token_service=token_service,
        )

        result = await handler.run(
            InitiateLogin(
                callback_url="http://localhost/callback",
                final_redirect_uri="http://localhost/dashboard",
                provider="orcid",
            )
        )
        assert result.authorization_url == "https://example.com/auth"
