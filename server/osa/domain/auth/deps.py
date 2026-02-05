"""FastAPI dependencies for authentication."""

from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from osa.config import Config
from osa.domain.auth.model.value import UserId

# HTTP Bearer token security scheme
security = HTTPBearer(auto_error=False)


class CurrentUser:
    """Authenticated user from JWT token."""

    def __init__(self, user_id: UserId, orcid_id: str):
        self.user_id = user_id
        self.orcid_id = orcid_id


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
    config: Config,
) -> CurrentUser:
    """Extract and validate current user from JWT token.

    Usage in routes:
        @router.get("/protected")
        async def protected_endpoint(
            current_user: Annotated[CurrentUser, Depends(get_current_user)],
        ):
            ...
    """
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "missing_token", "message": "Authorization header required"},
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        payload = jwt.decode(
            credentials.credentials,
            config.auth.jwt.secret,
            algorithms=[config.auth.jwt.algorithm],
            audience="authenticated",
        )
        user_id = UserId.model_validate(payload["sub"])
        orcid_id = payload["orcid_id"]
        return CurrentUser(user_id=user_id, orcid_id=orcid_id)

    except jwt.ExpiredSignatureError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "token_expired", "message": "Token has expired"},
            headers={"WWW-Authenticate": "Bearer"},
        ) from e

    except jwt.InvalidTokenError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "invalid_token", "message": "Invalid token"},
            headers={"WWW-Authenticate": "Bearer"},
        ) from e
