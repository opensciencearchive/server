"""Auth service for orchestrating authentication flows."""

import logging

from osa.domain.auth.model.identity import Identity
from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import TokenFamilyId, UserId
from osa.domain.auth.port.identity_provider import IdentityInfo, IdentityProvider
from osa.domain.auth.port.repository import (
    IdentityRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.domain.auth.service.token import TokenService
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
    _identity_repo: IdentityRepository
    _refresh_token_repo: RefreshTokenRepository
    _token_service: TokenService
    _outbox: Outbox

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
    ) -> tuple[User, Identity, str, str]:
        """Complete OAuth flow and issue tokens.

        Args:
            provider: The identity provider
            code: Authorization code from callback
            redirect_uri: Must match the one used in authorization

        Returns:
            Tuple of (user, identity, access_token, refresh_token)
        """
        # Exchange code for identity info
        identity_info = await provider.exchange_code(code, redirect_uri)

        # Find or create user and identity
        user, identity = await self._find_or_create_user(identity_info)

        # Create tokens
        access_token, refresh_token = await self._create_tokens(user, identity)

        logger.info(
            "User authenticated: user_id=%s, provider=%s, external_id=%s",
            user.id,
            identity.provider,
            identity.external_id,
        )

        return user, identity, access_token, refresh_token

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
        from osa.domain.shared.error import InvalidStateError

        token_hash = self._token_service.hash_token(refresh_token_raw)
        stored_token = await self._refresh_token_repo.get_by_token_hash(token_hash)

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

        # Get user and their ORCiD identity
        user = await self._user_repo.get(stored_token.user_id)
        if user is None:
            raise InvalidStateError("User not found", code="user_not_found")

        identities = await self._identity_repo.get_by_user_id(user.id)
        orcid_identity = next((i for i in identities if i.provider == "orcid"), None)

        if orcid_identity is None:
            raise InvalidStateError("User has no ORCiD identity", code="no_orcid_identity")

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
            orcid_id=orcid_identity.external_id,
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

    async def get_orcid_identity(self, user_id: UserId) -> Identity | None:
        """Get the ORCiD identity for a user."""
        identities = await self._identity_repo.get_by_user_id(user_id)
        return next((i for i in identities if i.provider == "orcid"), None)

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

    async def _find_or_create_user(self, identity_info: IdentityInfo) -> tuple[User, Identity]:
        """Find existing user by identity or create new one."""
        # Check if identity already exists
        existing_identity = await self._identity_repo.get_by_provider_and_external_id(
            identity_info.provider, identity_info.external_id
        )

        if existing_identity:
            # User exists, return them
            user = await self._user_repo.get(existing_identity.user_id)
            if user is None:
                # Orphaned identity - shouldn't happen with CASCADE
                raise RuntimeError(f"Identity exists without user: {existing_identity.id}")
            return user, existing_identity

        # Create new user and identity
        user = User.create(display_name=identity_info.display_name)
        await self._user_repo.save(user)

        identity = Identity.create(
            user_id=user.id,
            provider=identity_info.provider,
            external_id=identity_info.external_id,
            metadata=identity_info.raw_data,
        )
        await self._identity_repo.save(identity)

        logger.info(
            "New user created: user_id=%s, provider=%s",
            user.id,
            identity_info.provider,
        )

        return user, identity

    async def _create_tokens(self, user: User, identity: Identity) -> tuple[str, str]:
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
        access_token = self._token_service.create_access_token(
            user_id=user.id,
            orcid_id=identity.external_id,
        )

        return access_token, raw_token
