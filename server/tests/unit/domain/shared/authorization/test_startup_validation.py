"""Tests for startup validation of handler __auth__ declarations — T036.

Tests that all handlers either declare __auth__ or their command/query is __public__.
"""

import pytest

from osa.domain.auth.model.principal import Principal
from osa.domain.shared.authorization.policy import requires_role
from osa.domain.auth.model.role import Role
from osa.domain.shared.command import Command, CommandHandler, Result
from osa.domain.shared.error import ConfigurationError
from osa.domain.shared.query import Query, QueryHandler
from osa.domain.shared.query import Result as QueryResult


def validate_handlers() -> None:
    """Scan all registered handler subclasses for __auth__ declarations.

    Raises ConfigurationError if any handler is missing __auth__
    and its command/query is not __public__.
    """
    from osa.domain.shared.authorization.startup import validate_all_handlers

    validate_all_handlers()


class TestStartupValidation:
    def test_validation_catches_missing_auth_on_command_handler(self) -> None:
        """A CommandHandler without __auth__ on a non-public command should fail startup."""

        class UnprotectedCommand(Command):
            pass

        class UnprotectedResult(Result):
            pass

        class UnprotectedHandler(CommandHandler[UnprotectedCommand, UnprotectedResult]):
            async def run(self, cmd: UnprotectedCommand) -> UnprotectedResult:
                return UnprotectedResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        with pytest.raises(ConfigurationError, match="UnprotectedHandler"):
            _check_handler_class(UnprotectedHandler, UnprotectedCommand)

    def test_validation_passes_for_protected_handler(self) -> None:
        """A handler with __auth__ should pass validation."""

        class ProtectedCommand(Command):
            pass

        class ProtectedResult(Result):
            pass

        class ProtectedHandler(CommandHandler[ProtectedCommand, ProtectedResult]):
            __auth__ = requires_role(Role.ADMIN)
            _principal: Principal | None = None

            async def run(self, cmd: ProtectedCommand) -> ProtectedResult:
                return ProtectedResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        # Should not raise
        _check_handler_class(ProtectedHandler, ProtectedCommand)

    def test_validation_passes_for_public_command(self) -> None:
        """A handler for a __public__ command should pass even without __auth__."""
        from typing import ClassVar

        class PublicCommand(Command):
            __public__: ClassVar[bool] = True

        class PublicResult(Result):
            pass

        class PublicHandler(CommandHandler[PublicCommand, PublicResult]):
            async def run(self, cmd: PublicCommand) -> PublicResult:
                return PublicResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        # Should not raise
        _check_handler_class(PublicHandler, PublicCommand)

    def test_validation_catches_missing_auth_on_query_handler(self) -> None:
        """A QueryHandler without __auth__ on a non-public query should fail."""

        class UnprotectedQuery(Query):
            pass

        class UnprotectedQueryResult(QueryResult):
            pass

        class UnprotectedQueryHandler(QueryHandler[UnprotectedQuery, UnprotectedQueryResult]):
            async def run(self, cmd: UnprotectedQuery) -> UnprotectedQueryResult:
                return UnprotectedQueryResult()

        from osa.domain.shared.authorization.startup import _check_handler_class

        with pytest.raises(ConfigurationError, match="UnprotectedQueryHandler"):
            _check_handler_class(UnprotectedQueryHandler, UnprotectedQuery)
