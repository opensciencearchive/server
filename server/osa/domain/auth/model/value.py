"""Value objects for the auth domain."""

import re
from dataclasses import dataclass
from uuid import UUID, uuid4

from pydantic import RootModel, field_validator


class UserId(RootModel[UUID]):
    """Unique identifier for a User."""

    @classmethod
    def generate(cls) -> "UserId":
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.root)

    def __hash__(self) -> int:
        return hash(self.root)


class IdentityId(RootModel[UUID]):
    """Unique identifier for an Identity."""

    @classmethod
    def generate(cls) -> "IdentityId":
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.root)

    def __hash__(self) -> int:
        return hash(self.root)


class RefreshTokenId(RootModel[UUID]):
    """Unique identifier for a RefreshToken."""

    @classmethod
    def generate(cls) -> "RefreshTokenId":
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.root)

    def __hash__(self) -> int:
        return hash(self.root)


class TokenFamilyId(RootModel[UUID]):
    """Identifier for a token family.

    All refresh tokens from a single login session share a family_id.
    Used for theft detection: if a revoked token is reused, the entire
    family is invalidated.
    """

    @classmethod
    def generate(cls) -> "TokenFamilyId":
        return cls(uuid4())

    def __str__(self) -> str:
        return str(self.root)

    def __hash__(self) -> int:
        return hash(self.root)


ORCID_PATTERN = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")


@dataclass(frozen=True)
class CurrentUser:
    """Authenticated user context extracted from JWT token."""

    user_id: "UserId"
    orcid_id: str


class OrcidId(RootModel[str]):
    """An ORCiD identifier (e.g., 0000-0001-2345-6789).

    ORCiD IDs are 16-digit numbers displayed as four groups of four,
    with a checksum character (digit or X) at the end.
    """

    @field_validator("root")
    @classmethod
    def validate_orcid_format(cls, v: str) -> str:
        if not ORCID_PATTERN.match(v):
            raise ValueError(f"Invalid ORCiD format: {v}")
        return v

    def __str__(self) -> str:
        return self.root

    def __hash__(self) -> int:
        return hash(self.root)
