"""DeviceAuthorization entity for the OAuth device flow."""

import secrets
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from osa.domain.auth.model.value import DeviceAuthorizationId, UserCode, UserId
from osa.domain.shared.error import InvalidStateError
from osa.domain.shared.model.entity import Entity

# Default device code expiry: 15 minutes
DEVICE_CODE_EXPIRY_SECONDS = 900

# Default polling interval: 5 seconds
DEVICE_POLL_INTERVAL = 5


class DeviceAuthorizationStatus(StrEnum):
    """Status of a device authorization request."""

    PENDING = "pending"
    AUTHORIZED = "authorized"
    CONSUMED = "consumed"
    EXPIRED = "expired"


class DeviceAuthorization(Entity):
    """A pending device authorization request in the OAuth device flow.

    Invariants:
    - device_code is cryptographically random (64 hex chars)
    - user_code is validated via UserCode value object
    - user_id is None while pending, non-None when authorized
    - Once consumed, cannot be reused (prevents replay)

    Status transitions:
        pending → authorized    (user completes ORCID login)
        pending → expired       (TTL exceeded, cleanup task)
        authorized → consumed   (CLI exchanges device_code for tokens)
    """

    id: DeviceAuthorizationId
    device_code: str
    user_code: UserCode
    status: DeviceAuthorizationStatus
    user_id: UserId | None = None
    expires_at: datetime
    created_at: datetime

    @property
    def is_expired(self) -> bool:
        """Check if the device code has expired."""
        return datetime.now(UTC) >= self.expires_at

    @property
    def is_pending(self) -> bool:
        """Check if authorization is still pending."""
        return self.status == DeviceAuthorizationStatus.PENDING

    @property
    def is_authorized(self) -> bool:
        """Check if authorization has been granted."""
        return self.status == DeviceAuthorizationStatus.AUTHORIZED

    @property
    def is_consumed(self) -> bool:
        """Check if the authorization has been consumed."""
        return self.status == DeviceAuthorizationStatus.CONSUMED

    def authorize(self, user_id: UserId) -> None:
        """Mark this device authorization as authorized by a user.

        Raises:
            InvalidStateError: If not in pending status or expired
        """
        if self.status != DeviceAuthorizationStatus.PENDING:
            raise InvalidStateError(
                f"Cannot authorize from status {self.status}",
                code="invalid_device_state",
            )
        if self.is_expired:
            raise InvalidStateError(
                "Device authorization has expired",
                code="expired_token",
            )
        self.status = DeviceAuthorizationStatus.AUTHORIZED
        self.user_id = user_id

    def consume(self) -> None:
        """Mark this device authorization as consumed (tokens issued).

        Raises:
            InvalidStateError: If not in authorized status
        """
        if self.status != DeviceAuthorizationStatus.AUTHORIZED:
            raise InvalidStateError(
                f"Cannot consume from status {self.status}",
                code="invalid_device_state",
            )
        self.status = DeviceAuthorizationStatus.CONSUMED

    def mark_expired(self) -> None:
        """Mark this device authorization as expired.

        Raises:
            InvalidStateError: If already consumed
        """
        if self.status == DeviceAuthorizationStatus.CONSUMED:
            raise InvalidStateError(
                "Cannot expire a consumed authorization",
                code="invalid_device_state",
            )
        self.status = DeviceAuthorizationStatus.EXPIRED

    @classmethod
    def create(cls, user_code: UserCode) -> "DeviceAuthorization":
        """Create a new device authorization with generated codes.

        Args:
            user_code: Pre-generated user code (allows retry on collision)
        """
        now = datetime.now(UTC)
        return cls(
            id=DeviceAuthorizationId.generate(),
            device_code=secrets.token_hex(32),
            user_code=user_code,
            status=DeviceAuthorizationStatus.PENDING,
            user_id=None,
            expires_at=now + timedelta(seconds=DEVICE_CODE_EXPIRY_SECONDS),
            created_at=now,
        )
