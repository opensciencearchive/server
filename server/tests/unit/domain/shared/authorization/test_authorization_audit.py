"""Tests for authorization audit logging — T058.

Tests that PolicySet.guard() emits structured log entries for allow and deny decisions.
"""

import logging

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.auth.model.role import Role
from osa.domain.auth.model.value import ProviderIdentity, UserId
from osa.domain.shared.authorization.action import Action
from osa.domain.shared.authorization.policy_set import POLICY_SET
from osa.domain.shared.error import AuthorizationError


def _make_principal(roles: frozenset[Role], user_id: UserId | None = None) -> Principal:
    return Principal(
        user_id=user_id or UserId.generate(),
        identity=ProviderIdentity(provider="test", external_id="test-ext"),
        roles=roles,
    )


class TestAuthorizationAuditLogging:
    def test_guard_logs_allow(self, caplog: pytest.LogCaptureFixture) -> None:
        """Successful authorization should emit an info-level log."""
        principal = _make_principal(frozenset({Role.ADMIN}))

        with caplog.at_level(logging.DEBUG, logger="osa.domain.shared.authorization.policy_set"):
            POLICY_SET.guard(principal, Action.SCHEMA_CREATE)

        # Should have an allow log entry
        allow_messages = [r for r in caplog.records if "allowed" in r.message.lower()]
        assert len(allow_messages) >= 1
        record = allow_messages[0]
        assert str(principal.user_id) in record.message
        assert Action.SCHEMA_CREATE in record.message

    def test_guard_logs_deny(self, caplog: pytest.LogCaptureFixture) -> None:
        """Failed authorization should emit a warning-level log."""
        depositor = _make_principal(frozenset({Role.DEPOSITOR}))

        with caplog.at_level(logging.DEBUG, logger="osa.domain.shared.authorization.policy_set"):
            with pytest.raises(AuthorizationError):
                POLICY_SET.guard(depositor, Action.SCHEMA_CREATE)

        # Should have a deny log entry
        deny_messages = [r for r in caplog.records if "denied" in r.message.lower()]
        assert len(deny_messages) >= 1
        record = deny_messages[0]
        assert str(depositor.user_id) in record.message
        assert Action.SCHEMA_CREATE in record.message

    def test_guard_logs_deny_for_anonymous(self, caplog: pytest.LogCaptureFixture) -> None:
        """Authorization denial for anonymous users should also be logged."""
        with caplog.at_level(logging.DEBUG, logger="osa.domain.shared.authorization.policy_set"):
            with pytest.raises(AuthorizationError):
                POLICY_SET.guard(None, Action.DEPOSITION_CREATE)

        deny_messages = [r for r in caplog.records if "denied" in r.message.lower()]
        assert len(deny_messages) >= 1
