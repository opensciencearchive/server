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
from osa.domain.auth.model.value import CurrentUser
from osa.domain.auth.port.identity_provider import IdentityProvider
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
    """Response containing user info."""

    id: str
    display_name: str | None
    orcid_id: str


@router.get("/login")
async def initiate_login(
    request: Request,
    config: FromDishka[Config],
    identity_provider: FromDishka[IdentityProvider],
    token_service: FromDishka[TokenService],
    redirect_uri: Annotated[str | None, Query()] = None,
    provider: Annotated[str, Query()] = "orcid",
) -> Response:
    """Initiate OAuth login flow.

    Redirects to identity provider's authorization page.
    """
    if provider != "orcid":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_provider",
                "message": f"Unsupported provider: {provider}",
            },
        )

    # Determine callback URL
    callback_url = config.auth.callback_url
    if not callback_url:
        # Derive from request URL
        callback_url = str(request.url_for("handle_oauth_callback"))

    # Create signed state token (includes redirect_uri, expiry, and nonce)
    final_redirect = redirect_uri or config.frontend.url
    state = token_service.create_oauth_state(final_redirect)

    # Generate authorization URL
    authorization_url = identity_provider.get_authorization_url(
        state=state,
        redirect_uri=callback_url,
    )

    logger.info("OAuth login initiated, redirecting to IdP")
    return RedirectResponse(url=authorization_url, status_code=302)


@router.get("/callback")
async def handle_oauth_callback(
    request: Request,
    config: FromDishka[Config],
    auth_service: FromDishka[AuthService],
    identity_provider: FromDishka[IdentityProvider],
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

    final_redirect = token_service.verify_oauth_state(state)
    if final_redirect is None:
        logger.warning("OAuth state invalid or expired")
        error_params = urlencode(
            {
                "error": "oauth_state_invalid",
                "error_description": "Invalid or expired state parameter",
            }
        )
        return RedirectResponse(url=f"{frontend_url}/auth/error?{error_params}")

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

        # Complete OAuth flow
        user, identity, access_token, refresh_token = await auth_service.complete_oauth(
            provider=identity_provider,
            code=code,
            redirect_uri=callback_url,
        )

        # Build redirect URL with tokens in fragment
        token_params = urlencode(
            {
                "access_token": access_token,
                "refresh_token": refresh_token,
                "token_type": "Bearer",
                "expires_in": token_service.access_token_expire_seconds,
                "user_id": str(user.id),
                "display_name": user.display_name or "",
                "orcid_id": identity.external_id,
            }
        )

        redirect_url = f"{final_redirect}#auth={token_params}"
        logger.info("OAuth complete, user authenticated: user_id=%s", user.id)
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
    auth_service: FromDishka[AuthService],
    token_service: FromDishka[TokenService],
) -> TokenResponse:
    """Refresh access token using refresh token."""
    try:
        _user, access_token, new_refresh_token = await auth_service.refresh_tokens(
            body.refresh_token
        )
        return TokenResponse(
            access_token=access_token,
            refresh_token=new_refresh_token,
            expires_in=token_service.access_token_expire_seconds,
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
    auth_service: FromDishka[AuthService],
) -> LogoutResponse:
    """Logout and revoke refresh token."""
    success = await auth_service.logout(body.refresh_token)
    return LogoutResponse(success=success)


@router.get("/me", response_model=UserResponse)
async def get_me(
    current_user: FromDishka[CurrentUser],
    auth_service: FromDishka[AuthService],
) -> UserResponse:
    """Get current authenticated user information."""
    user = await auth_service.get_user_by_id(current_user.user_id)

    if user is None:
        raise HTTPException(
            status_code=401,
            detail={"code": "user_not_found", "message": "User not found"},
        )

    return UserResponse(
        id=str(user.id),
        display_name=user.display_name,
        orcid_id=current_user.orcid_id,
    )
