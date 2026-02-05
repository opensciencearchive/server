"""DI provider for auth domain."""

from uuid import UUID

import jwt
from dishka import from_context, provide
from fastapi import HTTPException
from starlette.requests import Request

from osa.config import Config
from osa.domain.auth.model.value import CurrentUser, UserId
from osa.domain.auth.port.repository import (
    IdentityRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.outbox import Outbox
from osa.util.di.base import Provider
from osa.util.di.scope import Scope


class AuthProvider(Provider):
    """DI provider for auth domain services and handlers."""

    request = from_context(provides=Request, scope=Scope.UOW)

    @provide(scope=Scope.UOW)
    def get_token_service(self, config: Config) -> TokenService:
        """Provide TokenService."""
        return TokenService(_config=config.auth.jwt)

    @provide(scope=Scope.UOW)
    def get_auth_service(
        self,
        user_repo: UserRepository,
        identity_repo: IdentityRepository,
        refresh_token_repo: RefreshTokenRepository,
        token_service: TokenService,
        outbox: Outbox,
    ) -> AuthService:
        """Provide AuthService."""
        return AuthService(
            _user_repo=user_repo,
            _identity_repo=identity_repo,
            _refresh_token_repo=refresh_token_repo,
            _token_service=token_service,
            _outbox=outbox,
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
                orcid_id=payload["orcid_id"],
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
