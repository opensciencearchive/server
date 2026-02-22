"""Tests for the system user constant."""

from uuid import UUID

from osa.domain.auth.model.value import SYSTEM_USER_ID, UserId


class TestSystemUserId:
    def test_system_user_id_is_nil_uuid(self):
        assert SYSTEM_USER_ID.root == UUID("00000000-0000-0000-0000-000000000000")

    def test_system_user_id_is_user_id_type(self):
        assert isinstance(SYSTEM_USER_ID, UserId)
