"""Authentication routes for OAuth login flow and device authorization."""

import logging
from html import escape
from pathlib import Path
from typing import Annotated
from urllib.parse import urlencode

from dishka import FromDishka
from dishka.integrations.fastapi import DishkaRoute
from fastapi import APIRouter, Form, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from pydantic import BaseModel

from osa.config import Config
from osa.domain.auth.command.device import (
    CompleteDeviceOAuth,
    CompleteDeviceOAuthHandler,
    InitiateDeviceAuth,
    InitiateDeviceAuthHandler,
    PollDeviceToken,
    PollDeviceTokenHandler,
    VerifyDeviceCode,
    VerifyDeviceCodeHandler,
)
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

# Load HTML templates at import time
_TEMPLATES_DIR = Path(__file__).parent.parent / "templates" / "device"
_VERIFY_HTML = (_TEMPLATES_DIR / "verify.html").read_text()
_COMPLETE_HTML = (_TEMPLATES_DIR / "complete.html").read_text()
_ERROR_HTML = (_TEMPLATES_DIR / "error.html").read_text()


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


class DeviceAuthorizationResponse(BaseModel):
    """Response for device authorization initiation."""

    device_code: str
    user_code: str
    verification_uri: str
    expires_in: int
    interval: int


class DeviceTokenRequest(BaseModel):
    """Request body for device token polling."""

    device_code: str
    grant_type: str


class DeviceTokenError(BaseModel):
    """Error response for device token polling (RFC 8628)."""

    error: str
    error_description: str | None = None


# ============================================================================
# Standard OAuth Routes
# ============================================================================


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
    device_handler: FromDishka[CompleteDeviceOAuthHandler],
    token_service: FromDishka[TokenService],
    code: Annotated[str | None, Query()] = None,
    state: Annotated[str | None, Query()] = None,
    error: Annotated[str | None, Query()] = None,
    error_description: Annotated[str | None, Query()] = None,
) -> Response:
    """Handle OAuth callback from identity provider.

    Exchanges authorization code for tokens and redirects to frontend.
    For device flow sessions (device_code in state), marks device as authorized
    and redirects to the success page instead.
    """
    frontend_url = config.frontend.url

    # Helper to build error redirect URL
    def _error_redirect(error_code: str, description: str) -> str:
        return f"{frontend_url}/auth/error?{urlencode({'error': error_code, 'error_description': description})}"

    def _device_error_redirect(description: str) -> str:
        return f"/api/v1/auth/device/error?{urlencode({'error_description': description})}"

    # Check for OAuth errors
    if error:
        logger.warning("OAuth error: %s - %s", error, error_description)
        return RedirectResponse(
            url=_error_redirect(error, error_description or "Authentication failed")
        )

    # Validate signed state token
    if not state:
        logger.warning("OAuth state missing")
        return RedirectResponse(
            url=_error_redirect("oauth_state_missing", "Missing state parameter")
        )

    state_data = token_service.verify_oauth_state(state)
    if state_data is None:
        logger.warning("OAuth state invalid or expired")
        return RedirectResponse(
            url=_error_redirect("oauth_state_invalid", "Invalid or expired state parameter")
        )

    final_redirect = state_data.redirect_uri
    provider = state_data.provider
    is_device_flow = state_data.device_code is not None

    if not code:
        logger.warning("OAuth callback missing code")
        if is_device_flow:
            return RedirectResponse(url=_device_error_redirect("Authorization code not provided"))
        return RedirectResponse(
            url=_error_redirect("missing_code", "Authorization code not provided")
        )

    try:
        # Determine callback URL (must match what was used in authorization)
        callback_url = config.auth.callback_url
        if not callback_url:
            callback_url = str(request.url_for("handle_oauth_callback"))

        if is_device_flow:
            # Device flow: resolve user without minting tokens, then authorize device
            device_code = state_data.device_code
            if device_code is None:
                return RedirectResponse(url=_device_error_redirect("Missing device code in state"))

            await device_handler.run(
                CompleteDeviceOAuth(
                    code=code,
                    callback_url=callback_url,
                    provider=provider,
                    device_code=device_code,
                )
            )
            return RedirectResponse(url="/api/v1/auth/device/complete", status_code=302)

        # Standard OAuth flow: complete and redirect with tokens
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
        if is_device_flow:
            return RedirectResponse(
                url=_device_error_redirect("Authentication failed. Please try again.")
            )
        return RedirectResponse(
            url=_error_redirect("oauth_error", "Authentication failed. Please try again.")
        )


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


# ============================================================================
# Device Flow Routes (RFC 8628)
# ============================================================================


@router.post("/device", response_model=DeviceAuthorizationResponse)
async def initiate_device_auth(
    request: Request,
    handler: FromDishka[InitiateDeviceAuthHandler],
) -> DeviceAuthorizationResponse:
    """Start a device authorization flow.

    CLI calls this to begin the device flow. Returns a device code (for polling),
    a user code (for the human), and a verification URL.
    """
    # Build verification URI from current request
    verification_uri_base = str(request.url_for("show_device_verification_page"))

    result = await handler.run(InitiateDeviceAuth(verification_uri_base=verification_uri_base))

    return DeviceAuthorizationResponse(
        device_code=result.device_code,
        user_code=result.user_code,
        verification_uri=result.verification_uri,
        expires_in=result.expires_in,
        interval=result.interval,
    )


@router.get("/device/verify")
async def show_device_verification_page(
    request: Request,
    code: Annotated[str | None, Query()] = None,
    error_message: Annotated[str | None, Query()] = None,
) -> HTMLResponse:
    """Display the code entry page for device flow verification."""
    action_url = str(request.url_for("submit_device_code"))
    prefilled_code = escape(code or "")
    error_html = ""
    if error_message:
        error_html = f'<p class="error">{escape(error_message)}</p>'

    html = _VERIFY_HTML.format(
        action_url=action_url,
        prefilled_code=prefilled_code,
        error_html=error_html,
    )
    return HTMLResponse(content=html)


@router.post("/device/verify")
async def submit_device_code(
    request: Request,
    config: FromDishka[Config],
    handler: FromDishka[VerifyDeviceCodeHandler],
    user_code: Annotated[str, Form()],
) -> Response:
    """Submit the user code from the verification page.

    Validates the code and redirects to ORCID OAuth flow if valid.
    """
    verify_url = str(request.url_for("show_device_verification_page"))

    callback_url = config.auth.callback_url
    if not callback_url:
        callback_url = str(request.url_for("handle_oauth_callback"))

    try:
        # TODO: make provider configurable instead of hardcoding "orcid"
        result = await handler.run(
            VerifyDeviceCode(
                user_code=user_code,
                callback_url=callback_url,
                provider="orcid",
            )
        )
        return RedirectResponse(url=result.authorization_url, status_code=302)
    except InvalidStateError:
        params = urlencode(
            {
                "code": user_code,
                "error_message": "Invalid or expired code. Check your terminal and try again.",
            }
        )
        return RedirectResponse(url=f"{verify_url}?{params}", status_code=302)


@router.post("/device/token")
async def poll_device_token(
    body: DeviceTokenRequest,
    handler: FromDishka[PollDeviceTokenHandler],
) -> Response:
    """Poll for device authorization completion.

    Returns tokens on success or RFC 8628 error codes.
    """
    try:
        result = await handler.run(
            PollDeviceToken(
                device_code=body.device_code,
                grant_type=body.grant_type,
            )
        )
        return JSONResponse(
            content={
                "access_token": result.access_token,
                "refresh_token": result.refresh_token,
                "token_type": result.token_type,
                "expires_in": result.expires_in,
            }
        )
    except InvalidStateError as e:
        # Map domain errors to RFC 8628 error codes
        return JSONResponse(
            status_code=400,
            content={
                "error": e.code,
                "error_description": e.message,
            },
        )


@router.get("/device/complete")
async def show_device_complete() -> HTMLResponse:
    """Success page after ORCID authentication in device flow."""
    return HTMLResponse(content=_COMPLETE_HTML)


@router.get("/device/error")
async def show_device_error(
    error_description: Annotated[str | None, Query()] = None,
) -> HTMLResponse:
    """Error page when device flow ORCID callback fails."""
    description = escape(error_description or "An unexpected error occurred.")
    html = _ERROR_HTML.format(error_description=description)
    return HTMLResponse(content=html)
