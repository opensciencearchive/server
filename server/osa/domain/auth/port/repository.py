"""Repository ports for the auth domain."""

from abc import abstractmethod
from typing import Protocol

from osa.domain.auth.model.identity import Identity
from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import (
    IdentityId,
    RefreshTokenId,
    TokenFamilyId,
    UserId,
)
from osa.domain.shared.port import Port


class UserRepository(Port, Protocol):
    """Repository for User aggregate persistence."""

    @abstractmethod
    async def get(self, user_id: UserId) -> User | None:
        """Get a user by ID."""
        ...

    @abstractmethod
    async def save(self, user: User) -> None:
        """Save a user (create or update)."""
        ...


class IdentityRepository(Port, Protocol):
    """Repository for Identity entity persistence."""

    @abstractmethod
    async def get(self, identity_id: IdentityId) -> Identity | None:
        """Get an identity by ID."""
        ...

    @abstractmethod
    async def get_by_provider_and_external_id(
        self, provider: str, external_id: str
    ) -> Identity | None:
        """Get an identity by provider and external ID."""
        ...

    @abstractmethod
    async def get_by_user_id(self, user_id: UserId) -> list[Identity]:
        """Get all identities for a user."""
        ...

    @abstractmethod
    async def save(self, identity: Identity) -> None:
        """Save an identity."""
        ...


class RefreshTokenRepository(Port, Protocol):
    """Repository for RefreshToken entity persistence."""

    @abstractmethod
    async def get(self, token_id: RefreshTokenId) -> RefreshToken | None:
        """Get a refresh token by ID."""
        ...

    @abstractmethod
    async def get_by_token_hash(
        self, token_hash: str, *, for_update: bool = False
    ) -> RefreshToken | None:
        """Get a refresh token by its hash.

        Args:
            token_hash: The hash of the token to find.
            for_update: If True, acquire a row-level lock (SELECT FOR UPDATE)
                to prevent concurrent modifications. Use this when the token
                will be modified after retrieval (e.g., during refresh).
        """
        ...

    @abstractmethod
    async def save(self, token: RefreshToken) -> None:
        """Save a refresh token."""
        ...

    @abstractmethod
    async def revoke_family(self, family_id: TokenFamilyId) -> int:
        """Revoke all tokens in a family. Returns count of revoked tokens."""
        ...
