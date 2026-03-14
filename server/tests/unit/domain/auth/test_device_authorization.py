"""Unit tests for DeviceAuthorization entity."""

from datetime import UTC, datetime, timedelta

import pytest

from osa.domain.auth.model.device_authorization import (
    DeviceAuthorization,
    DeviceAuthorizationStatus,
)
from osa.domain.auth.model.value import DeviceAuthorizationId, UserCode, UserId
from osa.domain.shared.error import InvalidStateError


def make_device_auth(
    *,
    status: DeviceAuthorizationStatus = DeviceAuthorizationStatus.PENDING,
    user_id: UserId | None = None,
    expired: bool = False,
) -> DeviceAuthorization:
    """Create a DeviceAuthorization for testing."""
    now = datetime.now(UTC)
    expires_at = now - timedelta(minutes=1) if expired else now + timedelta(minutes=15)
    return DeviceAuthorization(
        id=DeviceAuthorizationId.generate(),
        device_code="a" * 64,
        user_code=UserCode("BCDF2347"),
        status=status,
        user_id=user_id,
        expires_at=expires_at,
        created_at=now,
    )


class TestDeviceAuthorizationCreate:
    """Tests for DeviceAuthorization.create factory."""

    def test_create_generates_device_code(self):
        """create() should generate a 64-char hex device code."""
        auth = DeviceAuthorization.create(user_code=UserCode("BCDF2347"))
        assert len(auth.device_code) == 64
        assert all(c in "0123456789abcdef" for c in auth.device_code)

    def test_create_sets_pending_status(self):
        """create() should set status to pending."""
        auth = DeviceAuthorization.create(user_code=UserCode("BCDF2347"))
        assert auth.status == DeviceAuthorizationStatus.PENDING

    def test_create_user_id_is_none(self):
        """create() should set user_id to None."""
        auth = DeviceAuthorization.create(user_code=UserCode("BCDF2347"))
        assert auth.user_id is None

    def test_create_sets_expiry(self):
        """create() should set expires_at ~15 minutes in the future."""
        before = datetime.now(UTC)
        auth = DeviceAuthorization.create(user_code=UserCode("BCDF2347"))
        after = datetime.now(UTC)

        expected_min = before + timedelta(minutes=14, seconds=59)
        expected_max = after + timedelta(minutes=15, seconds=1)
        assert expected_min <= auth.expires_at <= expected_max

    def test_create_unique_device_codes(self):
        """create() should generate unique device codes each time."""
        auth1 = DeviceAuthorization.create(user_code=UserCode("BCDF2347"))
        auth2 = DeviceAuthorization.create(user_code=UserCode("BCDF2347"))
        assert auth1.device_code != auth2.device_code


class TestDeviceAuthorizationStatusTransitions:
    """Tests for status transitions."""

    def test_authorize_from_pending(self):
        """authorize() should transition pending → authorized."""
        auth = make_device_auth()
        user_id = UserId.generate()
        auth.authorize(user_id)

        assert auth.status == DeviceAuthorizationStatus.AUTHORIZED
        assert auth.user_id == user_id

    def test_authorize_rejects_non_pending(self):
        """authorize() should raise if not pending."""
        auth = make_device_auth(
            status=DeviceAuthorizationStatus.AUTHORIZED,
            user_id=UserId.generate(),
        )
        with pytest.raises(InvalidStateError, match="Cannot authorize from status"):
            auth.authorize(UserId.generate())

    def test_authorize_rejects_expired(self):
        """authorize() should raise if expired."""
        auth = make_device_auth(expired=True)
        with pytest.raises(InvalidStateError, match="expired"):
            auth.authorize(UserId.generate())

    def test_consume_from_authorized(self):
        """consume() should transition authorized → consumed."""
        auth = make_device_auth(
            status=DeviceAuthorizationStatus.AUTHORIZED,
            user_id=UserId.generate(),
        )
        auth.consume()
        assert auth.status == DeviceAuthorizationStatus.CONSUMED

    def test_consume_rejects_pending(self):
        """consume() should raise if not authorized."""
        auth = make_device_auth()
        with pytest.raises(InvalidStateError, match="Cannot consume from status"):
            auth.consume()

    def test_mark_expired_from_pending(self):
        """mark_expired() should transition pending → expired."""
        auth = make_device_auth()
        auth.mark_expired()
        assert auth.status == DeviceAuthorizationStatus.EXPIRED

    def test_mark_expired_rejects_consumed(self):
        """mark_expired() should raise if already consumed."""
        auth = make_device_auth(
            status=DeviceAuthorizationStatus.CONSUMED,
            user_id=UserId.generate(),
        )
        with pytest.raises(InvalidStateError, match="Cannot expire a consumed"):
            auth.mark_expired()


class TestDeviceAuthorizationProperties:
    """Tests for status query properties."""

    def test_is_pending(self):
        auth = make_device_auth()
        assert auth.is_pending is True
        assert auth.is_authorized is False
        assert auth.is_consumed is False

    def test_is_authorized(self):
        auth = make_device_auth(
            status=DeviceAuthorizationStatus.AUTHORIZED,
            user_id=UserId.generate(),
        )
        assert auth.is_pending is False
        assert auth.is_authorized is True

    def test_is_expired(self):
        auth = make_device_auth(expired=True)
        assert auth.is_expired is True

    def test_not_expired(self):
        auth = make_device_auth()
        assert auth.is_expired is False
