"""DI provider for auth domain."""

import logging
from uuid import UUID

import jwt
from dishka import from_context, provide
from fastapi import HTTPException
from starlette.requests import Request

from osa.config import Config
from osa.domain.auth.command.assign_role import AssignRoleHandler
from osa.domain.auth.command.login import (
    CompleteOAuthHandler,
    InitiateLoginHandler,
)
from osa.domain.auth.command.revoke_role import RevokeRoleHandler
from osa.domain.auth.command.token import LogoutHandler, RefreshTokensHandler
from osa.domain.auth.model.identity import Anonymous, Identity
from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import CurrentUser, ProviderIdentity, UserId
from osa.domain.auth.port.repository import (
    LinkedAccountRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.domain.auth.port.role_repository import RoleAssignmentRepository
from osa.domain.auth.query.get_user_roles import GetUserRolesHandler
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.authorization import AuthorizationService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.outbox import Outbox
from osa.util.di.base import Provider
from osa.util.di.scope import Scope

logger = logging.getLogger(__name__)


class AuthProvider(Provider):
    """DI provider for auth domain services and handlers."""

    request = from_context(provides=Request, scope=Scope.UOW)

    # Command Handlers
    initiate_login_handler = provide(InitiateLoginHandler, scope=Scope.UOW)
    complete_oauth_handler = provide(CompleteOAuthHandler, scope=Scope.UOW)
    refresh_tokens_handler = provide(RefreshTokensHandler, scope=Scope.UOW)
    logout_handler = provide(LogoutHandler, scope=Scope.UOW)
    assign_role_handler = provide(AssignRoleHandler, scope=Scope.UOW)
    revoke_role_handler = provide(RevokeRoleHandler, scope=Scope.UOW)

    # Query Handlers
    get_user_roles_handler = provide(GetUserRolesHandler, scope=Scope.UOW)

    # Services
    authorization_service = provide(AuthorizationService, scope=Scope.UOW)

    @provide(scope=Scope.UOW)
    def get_token_service(self, config: Config) -> TokenService:
        """Provide TokenService."""
        return TokenService(_config=config.auth.jwt)

    @provide(scope=Scope.UOW)
    def get_auth_service(
        self,
        config: Config,
        user_repo: UserRepository,
        linked_account_repo: LinkedAccountRepository,
        refresh_token_repo: RefreshTokenRepository,
        role_repo: RoleAssignmentRepository,
        token_service: TokenService,
        outbox: Outbox,
    ) -> AuthService:
        """Provide AuthService."""
        base_role = Role[config.auth.base_role] if config.auth.base_role else None
        logger.info("AuthService base_role config: %s -> %s", config.auth.base_role, base_role)
        return AuthService(
            _user_repo=user_repo,
            _linked_account_repo=linked_account_repo,
            _refresh_token_repo=refresh_token_repo,
            _role_repo=role_repo,
            _token_service=token_service,
            _outbox=outbox,
            _base_role=base_role,
        )

    @provide(scope=Scope.UOW)
    def get_current_user(
        self,
        request: Request,
        token_service: TokenService,
    ) -> CurrentUser:
        """Extract and validate CurrentUser from JWT in Authorization header.

        Raises:
            HTTPException: If token is missing, expired, or invalid
        """
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            raise HTTPException(
                status_code=401,
                detail={"code": "missing_token", "message": "Authorization header required"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        token = auth_header[7:]  # Remove "Bearer " prefix

        try:
            payload = token_service.validate_access_token(token)
            return CurrentUser(
                user_id=UserId(UUID(payload["sub"])),
                identity=ProviderIdentity(
                    provider=payload["provider"],
                    external_id=payload["external_id"],
                ),
            )
        except jwt.ExpiredSignatureError as e:
            raise HTTPException(
                status_code=401,
                detail={"code": "token_expired", "message": "Token has expired"},
                headers={"WWW-Authenticate": "Bearer"},
            ) from e
        except jwt.InvalidTokenError as e:
            raise HTTPException(
                status_code=401,
                detail={"code": "invalid_token", "message": "Invalid token"},
                headers={"WWW-Authenticate": "Bearer"},
            ) from e

    @provide(scope=Scope.UOW)
    async def get_identity(
        self,
        request: Request,
        token_service: TokenService,
        role_repo: RoleAssignmentRepository,
    ) -> Identity:
        """Resolve Identity from JWT + role lookup.

        Returns Anonymous for unauthenticated requests, Principal for authenticated.
        """
        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return Anonymous()

        token = auth_header[7:]  # Remove "Bearer " prefix

        try:
            payload = token_service.validate_access_token(token)
        except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
            return Anonymous()

        user_id = UserId(UUID(payload["sub"]))

        # Lookup roles from DB (includes base_role if assigned at user creation)
        assignments = await role_repo.get_by_user_id(user_id)
        roles = frozenset(a.role for a in assignments)
        logger.debug(
            "Identity resolved: user_id=%s, roles=%s, assignments=%d",
            user_id,
            roles,
            len(assignments),
        )

        return Principal(
            user_id=user_id,
            provider_identity=ProviderIdentity(
                provider=payload["provider"],
                external_id=payload["external_id"],
            ),
            roles=roles,
        )

    @provide(scope=Scope.UOW)
    def get_principal(self, identity: Identity) -> Principal:
        """Extract Principal from Identity. Raises if not authenticated."""
        from osa.domain.shared.error import AuthorizationError

        if isinstance(identity, Principal):
            return identity
        raise AuthorizationError("Authentication required", code="missing_token")
