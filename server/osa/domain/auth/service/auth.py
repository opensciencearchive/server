"""Auth service for orchestrating authentication flows."""

import logging
import secrets
from dataclasses import dataclass

from osa.domain.auth.model.device_authorization import DeviceAuthorization
from osa.domain.auth.model.linked_account import LinkedAccount
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.role_assignment import RoleAssignment
from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import (
    SAFE_CHARS,
    ProviderIdentity,
    TokenFamilyId,
    UserCode,
    UserId,
)
from osa.domain.auth.port.identity_provider import IdentityInfo, IdentityProvider
from osa.domain.auth.port.repository import (
    DeviceAuthorizationRepository,
    LinkedAccountRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.domain.auth.port.role_repository import RoleAssignmentRepository
from osa.domain.auth.service.token import TokenService
from osa.domain.shared.error import ConflictError, InfrastructureError, InvalidStateError
from osa.domain.shared.outbox import Outbox
from osa.domain.shared.service import Service

logger = logging.getLogger(__name__)


class AuthService(Service):
    """Orchestrates authentication flows.

    - initiate_login: Generate authorization URL
    - complete_oauth: Exchange code for tokens, create/update user
    - refresh_tokens: Issue new tokens from refresh token
    - logout: Revoke refresh token family
    """

    _user_repo: UserRepository
    _linked_account_repo: LinkedAccountRepository
    _refresh_token_repo: RefreshTokenRepository
    _role_repo: RoleAssignmentRepository
    _device_auth_repo: DeviceAuthorizationRepository
    _token_service: TokenService
    _outbox: Outbox
    _base_role: Role | None

    async def initiate_login(
        self,
        provider: IdentityProvider,
        state: str,
        redirect_uri: str,
    ) -> str:
        """Generate the authorization URL for OAuth login.

        Args:
            provider: The identity provider to use
            state: CSRF protection token (caller should store this)
            redirect_uri: Where the IdP should redirect after auth

        Returns:
            Authorization URL to redirect the user to
        """
        return provider.get_authorization_url(state, redirect_uri)

    async def complete_oauth(
        self,
        provider: IdentityProvider,
        code: str,
        redirect_uri: str,
    ) -> tuple[User, LinkedAccount, str, str]:
        """Complete OAuth flow and issue tokens.

        Args:
            provider: The identity provider
            code: Authorization code from callback
            redirect_uri: Must match the one used in authorization

        Returns:
            Tuple of (user, linked_account, access_token, refresh_token)
        """
        # Exchange code for identity info
        identity_info = await provider.exchange_code(code, redirect_uri)

        # Find or create user and linked account
        user, linked_account = await self._find_or_create_user(identity_info)

        # Create tokens
        access_token, refresh_token = await self._create_tokens(user, linked_account)

        logger.info(
            "User authenticated: user_id=%s, provider=%s, external_id=%s",
            user.id,
            linked_account.provider,
            linked_account.external_id,
        )

        return user, linked_account, access_token, refresh_token

    async def refresh_tokens(
        self,
        refresh_token_raw: str,
    ) -> tuple[User, str, str]:
        """Refresh access token using refresh token.

        Implements token rotation: old refresh token is revoked,
        new one issued in same family.

        Args:
            refresh_token_raw: The raw refresh token from client

        Returns:
            Tuple of (user, new_access_token, new_refresh_token)

        Raises:
            InvalidStateError: If refresh token is invalid, expired, or revoked
        """
        token_hash = self._token_service.hash_token(refresh_token_raw)
        # Lock the row to prevent concurrent refresh attempts (race condition)
        stored_token = await self._refresh_token_repo.get_by_token_hash(token_hash, for_update=True)

        if stored_token is None:
            raise InvalidStateError("Invalid refresh token", code="invalid_refresh_token")

        if stored_token.is_revoked:
            # Potential theft detected - revoke entire family
            await self._refresh_token_repo.revoke_family(stored_token.family_id)
            logger.warning(
                "Refresh token reuse detected, family revoked: family_id=%s",
                stored_token.family_id,
            )
            raise InvalidStateError(
                "Token family revoked - please login again",
                code="token_family_revoked",
            )

        if stored_token.is_expired:
            raise InvalidStateError("Refresh token expired", code="refresh_token_expired")

        # Revoke old token
        stored_token.revoke()
        await self._refresh_token_repo.save(stored_token)

        # Get user and their primary identity
        user = await self._user_repo.get(stored_token.user_id)
        if user is None:
            raise InvalidStateError("User not found", code="user_not_found")

        primary_identity = await self.get_primary_identity(user.id)

        if primary_identity is None:
            raise InvalidStateError("User has no identity", code="no_identity")

        # Issue new tokens in same family
        raw_token, token_hash = self._token_service.create_refresh_token()
        new_refresh_token = RefreshToken.create(
            user_id=user.id,
            token_hash=token_hash,
            family_id=stored_token.family_id,
            expires_in_days=self._token_service.refresh_token_expire_days,
        )
        await self._refresh_token_repo.save(new_refresh_token)

        access_token = self._token_service.create_access_token(
            user_id=user.id,
            identity=primary_identity,
        )

        logger.info("Tokens refreshed: user_id=%s", user.id)

        return user, access_token, raw_token

    async def logout(self, refresh_token_raw: str) -> bool:
        """Logout by revoking refresh token family.

        Args:
            refresh_token_raw: The raw refresh token

        Returns:
            True if tokens were revoked
        """
        token_hash = self._token_service.hash_token(refresh_token_raw)
        stored_token = await self._refresh_token_repo.get_by_token_hash(token_hash)

        if stored_token is None:
            # Token not found, but logout succeeds anyway
            return True

        revoked_count = await self._refresh_token_repo.revoke_family(stored_token.family_id)
        logger.info(
            "User logged out: user_id=%s, revoked_tokens=%d",
            stored_token.user_id,
            revoked_count,
        )

        return True

    async def get_user_by_id(self, user_id: UserId) -> User | None:
        """Get a user by their ID."""
        return await self._user_repo.get(user_id)

    async def get_primary_identity(self, user_id: UserId) -> ProviderIdentity | None:
        """Get the primary identity for a user.

        Returns the first identity found for the user. In the future,
        this could be extended to support multiple identities with a
        designated primary.
        """
        accounts = await self._linked_account_repo.get_by_user_id(user_id)
        if not accounts:
            return None
        first = accounts[0]
        return ProviderIdentity(provider=first.provider, external_id=first.external_id)

    async def get_user_id_from_refresh_token(self, raw_token: str) -> UserId | None:
        """Get the user ID associated with a refresh token.

        Args:
            raw_token: The raw refresh token string

        Returns:
            The user ID if token exists, None otherwise
        """
        token_hash = self._token_service.hash_token(raw_token)
        stored = await self._refresh_token_repo.get_by_token_hash(token_hash)
        return stored.user_id if stored else None

    # ========================================================================
    # Device Flow Methods
    # ========================================================================

    async def create_device_authorization(self) -> DeviceAuthorization:
        """Create a new device authorization with generated codes.

        Retries on user_code collision (unique constraint violation at DB level).

        Returns:
            The created DeviceAuthorization entity
        """
        max_retries = 5
        for attempt in range(max_retries):
            user_code = UserCode(self._generate_user_code())
            device_auth = DeviceAuthorization.create(user_code=user_code)

            try:
                await self._device_auth_repo.save(device_auth)
            except ConflictError:
                logger.info(
                    "User code collision on attempt %d, retrying",
                    attempt + 1,
                )
                continue

            logger.info(
                "Device authorization created: id=%s, user_code=%s",
                device_auth.id,
                user_code.display,
            )
            return device_auth

        raise InfrastructureError(
            f"Failed to generate unique user code after {max_retries} attempts",
            code="user_code_generation_failed",
        )

    async def verify_user_code(self, user_code: UserCode) -> DeviceAuthorization | None:
        """Look up a pending device authorization by user code.

        Returns None if not found or not in pending status.
        """
        device_auth = await self._device_auth_repo.get_by_user_code(user_code)
        if device_auth is None:
            return None
        if not device_auth.is_pending:
            return None
        if device_auth.is_expired:
            return None
        return device_auth

    async def authorize_device(self, device_code: str, user_id: UserId) -> None:
        """Mark a device authorization as authorized with the given user.

        Args:
            device_code: The device code to authorize
            user_id: The user who completed authentication

        Raises:
            InvalidStateError: If device code not found or in wrong state
        """
        device_auth = await self._device_auth_repo.get_by_device_code(device_code)
        if device_auth is None:
            raise InvalidStateError(
                "Device authorization not found",
                code="device_not_found",
            )

        if device_auth.is_expired:
            raise InvalidStateError(
                "Device authorization has expired",
                code="expired_token",
            )

        device_auth.authorize(user_id)
        await self._device_auth_repo.save(device_auth)

        logger.info(
            "Device authorized: device_code=%s..., user_id=%s",
            device_code[:8],
            user_id,
        )

    async def exchange_device_code(self, device_code: str) -> "DeviceTokenResult | None":
        """Exchange a device code for tokens.

        Mints a fresh access token and refresh token (new token family).
        Uses an atomic consume to prevent concurrent token issuance.

        Returns:
            DeviceTokenResult if authorized, None if still pending.

        Raises:
            InvalidStateError: If device code is expired, consumed, or not found
        """
        # Attempt atomic AUTHORIZED → CONSUMED transition.
        # Only one concurrent caller can succeed.
        device_auth = await self._device_auth_repo.consume_if_authorized(device_code)

        if device_auth is not None:
            # Successfully consumed — mint tokens
            if device_auth.user_id is None:
                raise InvalidStateError(
                    "Authorized device has no user_id",
                    code="invalid_device_state",
                )

            user = await self._user_repo.get(device_auth.user_id)
            if user is None:
                raise InvalidStateError("User not found", code="user_not_found")

            primary_identity = await self.get_primary_identity(user.id)
            if primary_identity is None:
                raise InvalidStateError("User has no identity", code="no_identity")

            # Create fresh token family for CLI session
            raw_token, token_hash = self._token_service.create_refresh_token()
            refresh_token = RefreshToken.create(
                user_id=user.id,
                token_hash=token_hash,
                family_id=TokenFamilyId.generate(),
                expires_in_days=self._token_service.refresh_token_expire_days,
            )
            await self._refresh_token_repo.save(refresh_token)

            access_token = self._token_service.create_access_token(
                user_id=user.id,
                identity=primary_identity,
            )

            logger.info("Device code exchanged for tokens: user_id=%s", user.id)
            return DeviceTokenResult(user=user, access_token=access_token, refresh_token=raw_token)

        # Atomic consume returned None — determine the specific error
        device_auth = await self._device_auth_repo.get_by_device_code(device_code)
        if device_auth is None:
            raise InvalidStateError(
                "Device authorization not found",
                code="device_not_found",
            )

        if device_auth.is_expired:
            raise InvalidStateError(
                "The device code has expired. Please start a new authorization.",
                code="expired_token",
            )

        if device_auth.is_consumed:
            raise InvalidStateError(
                "Device authorization already consumed",
                code="device_consumed",
            )

        if device_auth.is_pending:
            return None  # Not yet authorized — CLI should keep polling

        # Shouldn't reach here, but handle gracefully
        raise InvalidStateError(
            "Device authorization in unexpected state",
            code="invalid_device_state",
        )

    @staticmethod
    def _generate_user_code() -> str:
        """Generate a random 8-character user code from the safe character set."""
        return "".join(secrets.choice(SAFE_CHARS) for _ in range(8))

    async def complete_device_oauth(
        self,
        provider: IdentityProvider,
        code: str,
        redirect_uri: str,
        device_code: str,
    ) -> None:
        """Complete OAuth for device flow: resolve user and authorize device.

        Args:
            provider: The identity provider
            code: Authorization code from callback
            redirect_uri: Must match the one used in authorization
            device_code: The device code to authorize
        """
        identity_info = await provider.exchange_code(code, redirect_uri)
        user, _linked_account = await self._find_or_create_user(identity_info)
        await self.authorize_device(device_code, user.id)

        logger.info(
            "Device flow callback complete: user_id=%s, device_code=%s...",
            user.id,
            device_code[:8],
        )

    async def _find_or_create_user(self, identity_info: IdentityInfo) -> tuple[User, LinkedAccount]:
        """Find existing user by identity or create new one."""
        # Check if linked account already exists
        existing = await self._linked_account_repo.get_by_provider_and_external_id(
            identity_info.provider, identity_info.external_id
        )

        if existing:
            # User exists, return them
            user = await self._user_repo.get(existing.user_id)
            if user is None:
                raise InvalidStateError(
                    f"LinkedAccount exists without user: {existing.id}",
                    code="orphaned_linked_account",
                )
            return user, existing

        # Create new user and linked account
        user = User.create(display_name=identity_info.display_name)
        await self._user_repo.save(user)

        linked_account = LinkedAccount.create(
            user_id=user.id,
            provider=identity_info.provider,
            external_id=identity_info.external_id,
            metadata=identity_info.raw_data,
        )
        await self._linked_account_repo.save(linked_account)

        # Assign configured base role to new users
        if self._base_role is not None:
            assignment = RoleAssignment.create(
                user_id=user.id,
                role=self._base_role,
                assigned_by=user.id,
            )
            await self._role_repo.save(assignment)

        logger.info(
            "New user created: user_id=%s, provider=%s, base_role=%s",
            user.id,
            identity_info.provider,
            self._base_role.name if self._base_role else None,
        )

        return user, linked_account

    async def _create_tokens(self, user: User, linked_account: LinkedAccount) -> tuple[str, str]:
        """Create access and refresh tokens for a user."""
        # Create refresh token
        raw_token, token_hash = self._token_service.create_refresh_token()
        refresh_token = RefreshToken.create(
            user_id=user.id,
            token_hash=token_hash,
            family_id=TokenFamilyId.generate(),
            expires_in_days=self._token_service.refresh_token_expire_days,
        )
        await self._refresh_token_repo.save(refresh_token)

        # Create access token
        provider_identity = ProviderIdentity(
            provider=linked_account.provider,
            external_id=linked_account.external_id,
        )
        access_token = self._token_service.create_access_token(
            user_id=user.id,
            identity=provider_identity,
        )

        return access_token, raw_token


@dataclass(frozen=True)
class DeviceTokenResult:
    """Result of exchanging a device code for tokens."""

    user: User
    access_token: str
    refresh_token: str
