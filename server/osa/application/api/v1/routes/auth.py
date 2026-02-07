"""Authentication routes for OAuth login flow."""

import logging
from typing import Annotated
from urllib.parse import urlencode

from dishka import FromDishka
from dishka.integrations.fastapi import DishkaRoute
from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from osa.config import Config
from osa.domain.auth.command.login import (
    CompleteOAuth,
    CompleteOAuthHandler,
    InitiateLogin,
    InitiateLoginHandler,
)
from osa.domain.auth.command.token import (
    Logout,
    LogoutHandler,
    RefreshTokens,
    RefreshTokensHandler,
)
from osa.domain.auth.model.value import CurrentUser
from osa.domain.auth.port.provider_registry import ProviderRegistry
from osa.domain.auth.port.role_repository import RoleAssignmentRepository
from osa.domain.auth.service.auth import AuthService
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.error import InvalidStateError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/auth", tags=["Authentication"], route_class=DishkaRoute)


class RefreshTokenRequest(BaseModel):
    """Request body for token refresh."""

    refresh_token: str


class LogoutRequest(BaseModel):
    """Request body for logout."""

    refresh_token: str


class TokenResponse(BaseModel):
    """Response containing tokens."""

    access_token: str
    refresh_token: str
    token_type: str = "Bearer"
    expires_in: int


class LogoutResponse(BaseModel):
    """Response for logout."""

    success: bool


class UserResponse(BaseModel):
    """Response containing user info with roles."""

    id: str
    display_name: str | None
    provider: str
    external_id: str
    roles: list[str]


@router.get("/login")
async def initiate_login(
    request: Request,
    config: FromDishka[Config],
    handler: FromDishka[InitiateLoginHandler],
    registry: FromDishka[ProviderRegistry],
    provider: Annotated[str, Query()],
    redirect_uri: Annotated[str | None, Query()] = None,
) -> Response:
    """Initiate OAuth login flow.

    Redirects to identity provider's authorization page.
    """
    # Validate provider is configured
    if not registry.is_available(provider):
        available = registry.available_providers()
        raise HTTPException(
            status_code=400,
            detail={
                "code": "unknown_provider",
                "message": f"Unknown provider: {provider}. Available: {', '.join(available) or 'none'}",
            },
        )

    # Determine callback URL
    callback_url = config.auth.callback_url
    if not callback_url:
        callback_url = str(request.url_for("handle_oauth_callback"))

    # Determine final redirect URI
    final_redirect = redirect_uri or config.frontend.url

    result = await handler.run(
        InitiateLogin(
            callback_url=callback_url,
            final_redirect_uri=final_redirect,
            provider=provider,
        )
    )

    logger.info("OAuth login initiated for provider=%s, redirecting to IdP", provider)
    return RedirectResponse(url=result.authorization_url, status_code=302)


@router.get("/callback")
async def handle_oauth_callback(
    request: Request,
    config: FromDishka[Config],
    handler: FromDishka[CompleteOAuthHandler],
    token_service: FromDishka[TokenService],
    code: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    error: Annotated[str | None, Query()] = None,
    error_description: Annotated[str | None, Query()] = None,
) -> Response:
    """Handle OAuth callback from identity provider.

    Exchanges authorization code for tokens and redirects to frontend.
    """
    frontend_url = config.frontend.url

    # Check for OAuth errors
    if error:
        logger.warning("OAuth error: %s - %s", error, error_description)
        error_params = urlencode(
            {
                "error": error,
                "error_description": error_description or "Authentication failed",
            }
        )
        return RedirectResponse(url=f"{frontend_url}/auth/error?{error_params}")

    # Validate signed state token
    if not state:
        logger.warning("OAuth state missing")
        error_params = urlencode(
            {
                "error": "oauth_state_missing",
                "error_description": "Missing state parameter",
            }
        )
        return RedirectResponse(url=f"{frontend_url}/auth/error?{error_params}")

    state_data = token_service.verify_oauth_state(state)
    if state_data is None:
        logger.warning("OAuth state invalid or expired")
        error_params = urlencode(
            {
                "error": "oauth_state_invalid",
                "error_description": "Invalid or expired state parameter",
            }
        )
        return RedirectResponse(url=f"{frontend_url}/auth/error?{error_params}")

    final_redirect, provider = state_data

    if not code:
        logger.warning("OAuth callback missing code")
        error_params = urlencode(
            {
                "error": "missing_code",
                "error_description": "Authorization code not provided",
            }
        )
        return RedirectResponse(url=f"{frontend_url}/auth/error?{error_params}")

    try:
        # Determine callback URL (must match what was used in authorization)
        callback_url = config.auth.callback_url
        if not callback_url:
            callback_url = str(request.url_for("handle_oauth_callback"))

        # Complete OAuth flow via handler
        result = await handler.run(
            CompleteOAuth(
                code=code,
                callback_url=callback_url,
                provider=provider,
            )
        )

        # Build redirect URL with tokens in fragment
        token_params = urlencode(
            {
                "access_token": result.access_token,
                "refresh_token": result.refresh_token,
                "token_type": "Bearer",
                "expires_in": result.expires_in,
                "user_id": result.user_id,
                "display_name": result.display_name or "",
                "provider": result.provider,
                "external_id": result.external_id,
            }
        )

        redirect_url = f"{final_redirect}#auth={token_params}"
        logger.info(
            "OAuth complete, user authenticated: user_id=%s, provider=%s", result.user_id, provider
        )
        return RedirectResponse(url=redirect_url, status_code=302)

    except Exception as e:
        logger.exception("OAuth callback failed: %s", e)
        error_params = urlencode(
            {
                "error": "oauth_error",
                "error_description": "Authentication failed. Please try again.",
            }
        )
        return RedirectResponse(url=f"{frontend_url}/auth/error?{error_params}")


@router.post("/refresh", response_model=TokenResponse)
async def refresh_token(
    body: RefreshTokenRequest,
    handler: FromDishka[RefreshTokensHandler],
) -> TokenResponse:
    """Refresh access token using refresh token."""
    try:
        result = await handler.run(RefreshTokens(refresh_token=body.refresh_token))
        return TokenResponse(
            access_token=result.access_token,
            refresh_token=result.refresh_token,
            expires_in=result.expires_in,
        )
    except InvalidStateError as e:
        raise HTTPException(
            status_code=401,
            detail={
                "code": e.code,
                "message": e.message,
            },
        ) from e


@router.post("/logout", response_model=LogoutResponse)
async def logout(
    body: LogoutRequest,
    handler: FromDishka[LogoutHandler],
) -> LogoutResponse:
    """Logout and revoke refresh token."""
    result = await handler.run(Logout(refresh_token=body.refresh_token))
    return LogoutResponse(success=result.success)


@router.get("/me", response_model=UserResponse)
async def get_me(
    current_user: FromDishka[CurrentUser],
    auth_service: FromDishka[AuthService],
    role_repo: FromDishka[RoleAssignmentRepository],
) -> UserResponse:
    """Get current authenticated user information with roles."""
    user = await auth_service.get_user_by_id(current_user.user_id)

    if user is None:
        raise HTTPException(
            status_code=401,
            detail={"code": "user_not_found", "message": "User not found"},
        )

    assignments = await role_repo.get_by_user_id(current_user.user_id)
    roles = [a.role.name.lower() for a in assignments]

    return UserResponse(
        id=str(user.id),
        display_name=user.display_name,
        provider=current_user.identity.provider,
        external_id=current_user.identity.external_id,
        roles=roles,
    )
