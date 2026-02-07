"""PostgreSQL repository implementations for auth domain."""

from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from osa.domain.auth.model.linked_account import LinkedAccount
from osa.domain.auth.model.token import RefreshToken
from osa.domain.auth.model.user import User
from osa.domain.auth.model.value import (
    IdentityId,
    RefreshTokenId,
    TokenFamilyId,
    UserId,
)
from osa.domain.auth.port.repository import (
    LinkedAccountRepository,
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


def _row_to_linked_account(row: dict) -> LinkedAccount:
    """Convert a database row to a LinkedAccount model."""
    return LinkedAccount(
        id=IdentityId(UUID(row["id"])),
        user_id=UserId(UUID(row["user_id"])),
        provider=row["provider"],
        external_id=row["external_id"],
        metadata=row["metadata"],
        created_at=row["created_at"],
    )


def _linked_account_to_dict(account: LinkedAccount) -> dict:
    """Convert a LinkedAccount model to a database row dict."""
    return {
        "id": str(account.id),
        "user_id": str(account.user_id),
        "provider": account.provider,
        "external_id": account.external_id,
        "metadata": account.metadata,
        "created_at": account.created_at,
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


class PostgresLinkedAccountRepository(LinkedAccountRepository):
    """PostgreSQL implementation of LinkedAccountRepository."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get(self, identity_id: IdentityId) -> LinkedAccount | None:
        stmt = select(identities_table).where(identities_table.c.id == str(identity_id))
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_linked_account(dict(row)) if row else None

    async def get_by_provider_and_external_id(
        self, provider: str, external_id: str
    ) -> LinkedAccount | None:
        stmt = select(identities_table).where(
            identities_table.c.provider == provider,
            identities_table.c.external_id == external_id,
        )
        result = await self.session.execute(stmt)
        row = result.mappings().first()
        return _row_to_linked_account(dict(row)) if row else None

    async def get_by_user_id(self, user_id: UserId) -> list[LinkedAccount]:
        stmt = select(identities_table).where(identities_table.c.user_id == str(user_id))
        result = await self.session.execute(stmt)
        rows = result.mappings().all()
        return [_row_to_linked_account(dict(row)) for row in rows]

    async def save(self, linked_account: LinkedAccount) -> None:
        account_dict = _linked_account_to_dict(linked_account)
        existing = await self.get(linked_account.id)

        if existing:
            stmt = (
                update(identities_table)
                .where(identities_table.c.id == str(linked_account.id))
                .values(**account_dict)
            )
        else:
            stmt = insert(identities_table).values(**account_dict)

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

    async def get_by_token_hash(
        self, token_hash: str, *, for_update: bool = False
    ) -> RefreshToken | None:
        stmt = select(refresh_tokens_table).where(refresh_tokens_table.c.token_hash == token_hash)
        if for_update:
            stmt = stmt.with_for_update()
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
