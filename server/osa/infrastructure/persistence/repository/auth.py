"""PostgreSQL repository implementations for auth domain."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.auth.model.identity import Identity
from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import (
    IdentityId,
    RefreshTokenId,
    TokenFamilyId,
    UserId,
)
from osa.domain.auth.port.repository import (
    IdentityRepository,
    RefreshTokenRepository,
    UserRepository,
)
from osa.infrastructure.persistence.tables import (
    identities_table,
    refresh_tokens_table,
    users_table,
)


def _row_to_user(row: dict) -> User:
    """Convert a database row to a User model."""
    return User(
        id=UserId(UUID(row["id"])),
        display_name=row["display_name"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


def _user_to_dict(user: User) -> dict:
    """Convert a User model to a database row dict."""
    return {
        "id": str(user.id),
        "display_name": user.display_name,
        "created_at": user.created_at,
        "updated_at": user.updated_at,
    }


def _row_to_identity(row: dict) -> Identity:
    """Convert a database row to an Identity model."""
    return Identity(
        id=IdentityId(UUID(row["id"])),
        user_id=UserId(UUID(row["user_id"])),
        provider=row["provider"],
        external_id=row["external_id"],
        metadata=row["metadata"],
        created_at=row["created_at"],
    )


def _identity_to_dict(identity: Identity) -> dict:
    """Convert an Identity model to a database row dict."""
    return {
        "id": str(identity.id),
        "user_id": str(identity.user_id),
        "provider": identity.provider,
        "external_id": identity.external_id,
        "metadata": identity.metadata,
        "created_at": identity.created_at,
    }


def _row_to_refresh_token(row: dict) -> RefreshToken:
    """Convert a database row to a RefreshToken model."""
    return RefreshToken(
        id=RefreshTokenId(UUID(row["id"])),
        user_id=UserId(UUID(row["user_id"])),
        token_hash=row["token_hash"],
        family_id=TokenFamilyId(UUID(row["family_id"])),
        expires_at=row["expires_at"],
        created_at=row["created_at"],
        revoked_at=row["revoked_at"],
    )


def _refresh_token_to_dict(token: RefreshToken) -> dict:
    """Convert a RefreshToken model to a database row dict."""
    return {
        "id": str(token.id),
        "user_id": str(token.user_id),
        "token_hash": token.token_hash,
        "family_id": str(token.family_id),
        "expires_at": token.expires_at,
        "created_at": token.created_at,
        "revoked_at": token.revoked_at,
    }


class PostgresUserRepository(UserRepository):
    """PostgreSQL implementation of UserRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, user_id: UserId) -> User | None:
        stmt = select(users_table).where(users_table.c.id == str(user_id))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_user(dict(row)) if row else None

    async def save(self, user: User) -> None:
        user_dict = _user_to_dict(user)
        existing = await self.get(user.id)

        if existing:
            stmt = update(users_table).where(users_table.c.id == str(user.id)).values(**user_dict)
        else:
            stmt = insert(users_table).values(**user_dict)

        await self.session.execute(stmt)
        await self.session.flush()


class PostgresIdentityRepository(IdentityRepository):
    """PostgreSQL implementation of IdentityRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, identity_id: IdentityId) -> Identity | None:
        stmt = select(identities_table).where(identities_table.c.id == str(identity_id))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_identity(dict(row)) if row else None

    async def get_by_provider_and_external_id(
        self, provider: str, external_id: str
    ) -> Identity | None:
        stmt = select(identities_table).where(
            identities_table.c.provider == provider,
            identities_table.c.external_id == external_id,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_identity(dict(row)) if row else None

    async def get_by_user_id(self, user_id: UserId) -> list[Identity]:
        stmt = select(identities_table).where(identities_table.c.user_id == str(user_id))
        result = await self.session.execute(stmt)
        rows = result.mappings().all()
        return [_row_to_identity(dict(row)) for row in rows]

    async def save(self, identity: Identity) -> None:
        identity_dict = _identity_to_dict(identity)
        existing = await self.get(identity.id)

        if existing:
            stmt = (
                update(identities_table)
                .where(identities_table.c.id == str(identity.id))
                .values(**identity_dict)
            )
        else:
            stmt = insert(identities_table).values(**identity_dict)

        await self.session.execute(stmt)
        await self.session.flush()


class PostgresRefreshTokenRepository(RefreshTokenRepository):
    """PostgreSQL implementation of RefreshTokenRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, token_id: RefreshTokenId) -> RefreshToken | None:
        stmt = select(refresh_tokens_table).where(refresh_tokens_table.c.id == str(token_id))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_refresh_token(dict(row)) if row else None

    async def get_by_token_hash(self, token_hash: str) -> RefreshToken | None:
        stmt = select(refresh_tokens_table).where(refresh_tokens_table.c.token_hash == token_hash)
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_refresh_token(dict(row)) if row else None

    async def save(self, token: RefreshToken) -> None:
        token_dict = _refresh_token_to_dict(token)
        existing = await self.get(token.id)

        if existing:
            stmt = (
                update(refresh_tokens_table)
                .where(refresh_tokens_table.c.id == str(token.id))
                .values(**token_dict)
            )
        else:
            stmt = insert(refresh_tokens_table).values(**token_dict)

        await self.session.execute(stmt)
        await self.session.flush()

    async def revoke_family(self, family_id: TokenFamilyId) -> int:
        """Revoke all tokens in a family. Returns count of revoked tokens."""
        now = datetime.now(UTC)
        stmt = (
            update(refresh_tokens_table)
            .where(
                refresh_tokens_table.c.family_id == str(family_id),
                refresh_tokens_table.c.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )
        result = await self.session.execute(stmt)
        await self.session.flush()
        return result.rowcount
